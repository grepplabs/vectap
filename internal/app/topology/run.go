package topology

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"text/tabwriter"

	"github.com/grepplabs/vectap/internal/app/runconfig"
	"github.com/grepplabs/vectap/internal/forward"
	"github.com/grepplabs/vectap/internal/kube"
	"github.com/grepplabs/vectap/internal/targets"
	"github.com/grepplabs/vectap/internal/vectorapi"
	"gopkg.in/yaml.v3"
)

type Runner struct {
	client vectorapi.Client
}

//nolint:tagliatelle
type ListedTopology struct {
	TargetID            string   `json:"target_id" yaml:"target_id"`
	Namespace           string   `json:"namespace" yaml:"namespace"`
	PodName             string   `json:"pod_name" yaml:"pod_name"`
	ComponentID         string   `json:"component_id" yaml:"component_id"`
	ComponentKind       string   `json:"component_kind" yaml:"component_kind"`
	ComponentType       string   `json:"component_type" yaml:"component_type"`
	ReceivedBytesTotal  *float64 `json:"received_bytes_total,omitempty" yaml:"received_bytes_total,omitempty"`
	SentBytesTotal      *float64 `json:"sent_bytes_total,omitempty" yaml:"sent_bytes_total,omitempty"`
	ReceivedEventsTotal *float64 `json:"received_events_total,omitempty" yaml:"received_events_total,omitempty"`
	SentEventsTotal     *float64 `json:"sent_events_total,omitempty" yaml:"sent_events_total,omitempty"`
	Sources             []string `json:"sources,omitempty" yaml:"sources,omitempty"`
	Transforms          []string `json:"transforms,omitempty" yaml:"transforms,omitempty"`
	Sinks               []string `json:"sinks,omitempty" yaml:"sinks,omitempty"`
	Outputs             []string `json:"outputs,omitempty" yaml:"outputs,omitempty"`
	Parents             []string `json:"parents,omitempty" yaml:"parents,omitempty"`
	Children            []string `json:"children,omitempty" yaml:"children,omitempty"`
}

func NewDefaultRunner() *Runner {
	return &Runner{client: vectorapi.NewGraphQLWSClient()}
}

func NewRunner(client vectorapi.Client) *Runner {
	return &Runner{client: client}
}

func (r *Runner) Topology(ctx context.Context, cfg Config) error {
	runCfgs, err := expandRunConfigs(cfg)
	if err != nil {
		return err
	}

	effectiveFormat, err := formatForRender(cfg, runCfgs)
	if err != nil {
		return err
	}
	includeMeta := includeMetaForRender(cfg, runCfgs)

	var listed []ListedTopology
	for _, rcfg := range runCfgs {
		items, err := r.listSourceTopology(ctx, rcfg)
		if err != nil {
			return err
		}
		listed = append(listed, items...)
	}

	sort.Slice(listed, func(i, j int) bool {
		if listed[i].Namespace != listed[j].Namespace {
			return listed[i].Namespace < listed[j].Namespace
		}
		if listed[i].PodName != listed[j].PodName {
			return listed[i].PodName < listed[j].PodName
		}
		if listed[i].ComponentKind != listed[j].ComponentKind {
			return listed[i].ComponentKind < listed[j].ComponentKind
		}
		if listed[i].ComponentType != listed[j].ComponentType {
			return listed[i].ComponentType < listed[j].ComponentType
		}
		return listed[i].ComponentID < listed[j].ComponentID
	})
	if cfg.Orphaned {
		listed = onlyOrphaned(listed)
	}

	return render(os.Stdout, effectiveFormat, cfg.View, includeMeta, listed)
}

// nolint:cyclop
func (r *Runner) listSourceTopology(ctx context.Context, cfg Config) ([]ListedTopology, error) {
	switch cfg.Type {
	case runconfig.SourceTypeDirect:
		var out []ListedTopology
		for i, endpointURL := range cfg.DirectURLs {
			target := directTarget(i, endpointURL)
			items, err := r.client.Topology(ctx, endpointURL, vectorapi.TopologyRequest{})
			if err != nil {
				return nil, err
			}
			out = append(out, toListedTopology(target, cfg.SourceName, items)...)
		}
		return out, nil
	case runconfig.SourceTypeKubernetes:
		resolver, err := kube.NewResolverFromConfig(cfg.KubeConfigPath, cfg.KubeContext)
		if err != nil {
			return nil, err
		}
		fwd, err := forward.NewManagerFromConfig(cfg.KubeConfigPath, cfg.KubeContext)
		if err != nil {
			return nil, err
		}

		ts, err := resolver.Resolve(ctx, targets.ResolveOptions{
			Namespace:     cfg.Namespace,
			LabelSelector: cfg.LabelSelector,
			RemotePort:    cfg.VectorPort,
		})
		if err != nil {
			return nil, err
		}
		if len(ts) == 0 {
			return nil, errors.New("no matching targets found")
		}

		var out []ListedTopology
		for _, t := range ts {
			session, err := fwd.Start(ctx, t)
			if err != nil {
				return nil, fmt.Errorf("start port-forward for %s: %w", t.ID, err)
			}
			items, err := r.client.Topology(ctx, session.EndpointURL, vectorapi.TopologyRequest{})
			if err != nil {
				return nil, err
			}
			out = append(out, toListedTopology(t, cfg.SourceName, items)...)
		}
		return out, nil
	default:
		return nil, fmt.Errorf("unsupported type %q", cfg.Type)
	}
}

func expandRunConfigs(cfg Config) ([]Config, error) {
	if !cfg.UsesConfiguredSources() {
		return []Config{cfg}, nil
	}
	selected, err := runconfig.Select(
		cfg.AllSources,
		cfg.SelectedSources,
		cfg.Sources,
		func(s SourceConfig) string { return s.Name },
		func(s SourceConfig) bool { return s.Enabled },
	)
	if err != nil {
		return nil, err
	}

	out := make([]Config, 0, len(selected))
	for _, s := range selected {
		sc := cfg
		sc.Type = s.Type
		sc.DirectURLs = append([]string{}, s.DirectURLs...)
		sc.Namespace = s.Namespace
		sc.LabelSelector = s.LabelSelector
		sc.KubeConfigPath = s.KubeConfigPath
		sc.KubeContext = s.KubeContext
		sc.VectorPort = s.VectorPort
		sc.Format = s.Format
		sc.IncludeMeta = s.IncludeMeta
		sc.SourceName = s.Name
		sc.Sources = nil
		sc.SelectedSources = nil
		sc.AllSources = false
		out = append(out, sc)
	}
	return out, nil
}

func formatForRender(cfg Config, runCfgs []Config) (string, error) {
	if cfg.UsesConfiguredSources() {
		if len(runCfgs) == 0 {
			return cfg.Format, nil
		}
		format := runCfgs[0].Format
		if format == "" {
			format = runconfig.FormatText
		}
		for i := 1; i < len(runCfgs); i++ {
			candidate := runCfgs[i].Format
			if candidate == "" {
				candidate = runconfig.FormatText
			}
			if candidate != format {
				return "", fmt.Errorf("selected sources use different formats (%q and %q); set a single --format value", format, candidate)
			}
		}
		return format, nil
	}
	if cfg.Format == "" {
		return runconfig.FormatText, nil
	}
	return cfg.Format, nil
}

func toListedTopology(target targets.Target, sourceName string, components []vectorapi.TopologyComponent) []ListedTopology {
	out := make([]ListedTopology, 0, len(components))
	namespace := target.Namespace
	if sourceName != "" {
		namespace = sourceName + "/" + namespace
	}

	inferredParents, inferredChildren := inferComponentRelations(components)

	for _, c := range components {
		sources := componentRefIDs(c.Sources)
		transforms := componentRefIDs(c.Transforms)
		sinks := componentRefIDs(c.Sinks)
		outputs := outputIDs(c.Outputs)

		parents, children := listedTopologyLinks(c.ComponentKind, sources, transforms, sinks)
		parents = mergeIDs(parents, inferredParents[c.ComponentID])
		children = mergeIDs(children, inferredChildren[c.ComponentID])

		out = append(out, ListedTopology{
			TargetID:            target.ID,
			Namespace:           namespace,
			PodName:             target.PodName,
			ComponentID:         c.ComponentID,
			ComponentKind:       c.ComponentKind,
			ComponentType:       c.ComponentType,
			ReceivedBytesTotal:  c.ReceivedBytesTotal,
			SentBytesTotal:      c.SentBytesTotal,
			ReceivedEventsTotal: c.ReceivedEventsTotal,
			SentEventsTotal:     c.SentEventsTotal,
			Sources:             sources,
			Transforms:          transforms,
			Sinks:               sinks,
			Outputs:             outputs,
			Parents:             parents,
			Children:            children,
		})
	}
	return out
}

func inferComponentRelations(components []vectorapi.TopologyComponent) (map[string][]string, map[string][]string) {
	inferredChildren := make(map[string][]string, len(components))
	inferredParents := make(map[string][]string, len(components))
	for _, c := range components {
		id := c.ComponentID
		addInferredLinks(inferredParents, inferredChildren, mergeIDs(componentRefIDs(c.Sources), componentRefIDs(c.Transforms)), id, false)
		addInferredLinks(inferredParents, inferredChildren, mergeIDs(componentRefIDs(c.Transforms), componentRefIDs(c.Sinks)), id, true)
	}
	return inferredParents, inferredChildren
}

func addInferredLinks(inferredParents, inferredChildren map[string][]string, relatedIDs []string, componentID string, componentAsParent bool) {
	for _, relatedID := range relatedIDs {
		if relatedID == "" || relatedID == componentID {
			continue
		}
		if componentAsParent {
			inferredParents[relatedID] = append(inferredParents[relatedID], componentID)
			inferredChildren[componentID] = append(inferredChildren[componentID], relatedID)
			continue
		}
		inferredChildren[relatedID] = append(inferredChildren[relatedID], componentID)
		inferredParents[componentID] = append(inferredParents[componentID], relatedID)
	}
}

func listedTopologyLinks(kind string, sources, transforms, sinks []string) ([]string, []string) {
	switch kind {
	case "sink":
		return mergeIDs(sources, transforms), nil
	case "transform":
		return mergeIDs(sources, transforms), mergeIDs(transforms, sinks)
	default:
		return mergeIDs(sources, nil), mergeIDs(transforms, sinks)
	}
}

func includeMetaForRender(cfg Config, runCfgs []Config) bool {
	if cfg.UsesConfiguredSources() {
		for _, rcfg := range runCfgs {
			if rcfg.IncludeMeta {
				return true
			}
		}
		return false
	}
	return cfg.IncludeMeta
}

// nolint:cyclop
func render(w io.Writer, format, view string, includeMeta bool, listed []ListedTopology) error {
	switch format {
	case runconfig.FormatText:
		switch view {
		case "", ViewTable:
			return renderTable(w, includeMeta, listed)
		case ViewEdges:
			return renderEdges(w, includeMeta, listed)
		case ViewTree:
			return renderTree(w, includeMeta, listed)
		default:
			return fmt.Errorf("unsupported view %q", view)
		}
	case runconfig.FormatJSON:
		enc := json.NewEncoder(w)
		return enc.Encode(stripMeta(listed, includeMeta))
	case runconfig.FormatYAML:
		enc := yaml.NewEncoder(w)
		defer enc.Close() //nolint:errcheck
		return enc.Encode(stripMeta(listed, includeMeta))
	default:
		return fmt.Errorf("unsupported format %q", format)
	}
}

func renderTable(w io.Writer, includeMeta bool, listed []ListedTopology) error {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	header := "COMPONENT_ID\tKIND\tTYPE\tSRC_SENT_EVENTS\tSRC_SENT_BYTES\tDST_RECV_EVENTS\tDST_RECV_BYTES\tPARENTS\tCHILDREN\tSOURCES\tTRANSFORMS\tSINKS\tOUTPUTS"
	if includeMeta {
		header = "TARGET\tCOMPONENT_ID\tKIND\tTYPE\tSRC_SENT_EVENTS\tSRC_SENT_BYTES\tDST_RECV_EVENTS\tDST_RECV_BYTES\tPARENTS\tCHILDREN\tSOURCES\tTRANSFORMS\tSINKS\tOUTPUTS"
	}
	if _, err := fmt.Fprintln(tw, header); err != nil {
		return err
	}
	for _, item := range listed {
		if includeMeta {
			if _, err := fmt.Fprintf(
				tw,
				"%s/%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
				item.Namespace,
				item.PodName,
				item.ComponentID,
				item.ComponentKind,
				item.ComponentType,
				formatMetric(item.SentEventsTotal),
				formatMetric(item.SentBytesTotal),
				formatMetric(item.ReceivedEventsTotal),
				formatMetric(item.ReceivedBytesTotal),
				strings.Join(item.Parents, ","),
				strings.Join(item.Children, ","),
				strings.Join(item.Sources, ","),
				strings.Join(item.Transforms, ","),
				strings.Join(item.Sinks, ","),
				strings.Join(item.Outputs, ","),
			); err != nil {
				return err
			}
			continue
		}
		if _, err := fmt.Fprintf(
			tw,
			"%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
			item.ComponentID,
			item.ComponentKind,
			item.ComponentType,
			formatMetric(item.SentEventsTotal),
			formatMetric(item.SentBytesTotal),
			formatMetric(item.ReceivedEventsTotal),
			formatMetric(item.ReceivedBytesTotal),
			strings.Join(item.Parents, ","),
			strings.Join(item.Children, ","),
			strings.Join(item.Sources, ","),
			strings.Join(item.Transforms, ","),
			strings.Join(item.Sinks, ","),
			strings.Join(item.Outputs, ","),
		); err != nil {
			return err
		}
	}
	return tw.Flush()
}

func renderEdges(w io.Writer, includeMeta bool, listed []ListedTopology) error {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if err := writeEdgesHeader(tw, includeMeta); err != nil {
		return err
	}

	rows := buildEdgeRows(listed)
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].target != rows[j].target {
			return rows[i].target < rows[j].target
		}
		if rows[i].from != rows[j].from {
			return rows[i].from < rows[j].from
		}
		if rows[i].to != rows[j].to {
			return rows[i].to < rows[j].to
		}
		if rows[i].toK != rows[j].toK {
			return rows[i].toK < rows[j].toK
		}
		return rows[i].toType < rows[j].toType
	})

	for _, row := range rows {
		if err := writeEdgeRow(tw, includeMeta, row); err != nil {
			return err
		}
	}
	return tw.Flush()
}

type edgeRow struct {
	target          string
	from            string
	fromK           string
	fromType        string
	fromSent        *float64
	fromSentBytes   *float64
	to              string
	toK             string
	toType          string
	toReceived      *float64
	toReceivedBytes *float64
}

type edgeGroupIndex struct {
	kindByID          map[string]string
	typeByID          map[string]string
	receivedByID      map[string]*float64
	sentByID          map[string]*float64
	receivedBytesByID map[string]*float64
	sentBytesByID     map[string]*float64
}

func writeEdgesHeader(w io.Writer, includeMeta bool) error {
	header := "SRC\tSRC_KIND\tSRC_TYPE\tSRC_SENT_EVENTS\tSRC_SENT_BYTES\tDST\tDST_KIND\tDST_TYPE\tDST_RECV_EVENTS\tDST_RECV_BYTES"
	if includeMeta {
		header = "TARGET\tSRC\tSRC_KIND\tSRC_TYPE\tSRC_SENT_EVENTS\tSRC_SENT_BYTES\tDST\tDST_KIND\tDST_TYPE\tDST_RECV_EVENTS\tDST_RECV_BYTES"
	}
	_, err := fmt.Fprintln(w, header)
	return err
}

func buildEdgeRows(listed []ListedTopology) []edgeRow {
	grouped := map[string][]ListedTopology{}
	for _, item := range listed {
		grouped[topologyGroupKey(item)] = append(grouped[topologyGroupKey(item)], item)
	}

	groupKeys := make([]string, 0, len(grouped))
	for k := range grouped {
		groupKeys = append(groupKeys, k)
	}
	sort.Strings(groupKeys)

	rows := make([]edgeRow, 0, len(listed))
	seen := map[string]struct{}{}
	for _, groupKey := range groupKeys {
		items := grouped[groupKey]
		index := indexEdgeGroup(items)
		rows = append(rows, buildGroupEdgeRows(items, index, seen)...)
	}
	return rows
}

func indexEdgeGroup(items []ListedTopology) edgeGroupIndex {
	index := edgeGroupIndex{
		kindByID:          map[string]string{},
		typeByID:          map[string]string{},
		receivedByID:      map[string]*float64{},
		sentByID:          map[string]*float64{},
		receivedBytesByID: map[string]*float64{},
		sentBytesByID:     map[string]*float64{},
	}
	for _, item := range items {
		index.kindByID[item.ComponentID] = item.ComponentKind
		index.typeByID[item.ComponentID] = item.ComponentType
		index.receivedByID[item.ComponentID] = item.ReceivedEventsTotal
		index.sentByID[item.ComponentID] = item.SentEventsTotal
		index.receivedBytesByID[item.ComponentID] = item.ReceivedBytesTotal
		index.sentBytesByID[item.ComponentID] = item.SentBytesTotal
	}
	return index
}

func buildGroupEdgeRows(items []ListedTopology, index edgeGroupIndex, seen map[string]struct{}) []edgeRow {
	rows := make([]edgeRow, 0)
	for _, item := range items {
		for _, child := range item.Children {
			row := edgeRow{
				target:          topologyTargetLabel(item),
				from:            item.ComponentID,
				fromK:           item.ComponentKind,
				fromType:        item.ComponentType,
				fromSent:        index.sentByID[item.ComponentID],
				fromSentBytes:   index.sentBytesByID[item.ComponentID],
				to:              child,
				toK:             index.kindByID[child],
				toType:          index.typeByID[child],
				toReceived:      index.receivedByID[child],
				toReceivedBytes: index.receivedBytesByID[child],
			}
			if edgeSeen(seen, row) {
				continue
			}
			rows = append(rows, row)
		}
	}
	return rows
}

func edgeSeen(seen map[string]struct{}, row edgeRow) bool {
	key := strings.Join([]string{row.target, row.from, row.to}, "\x00")
	if _, ok := seen[key]; ok {
		return true
	}
	seen[key] = struct{}{}
	return false
}

func writeEdgeRow(w io.Writer, includeMeta bool, row edgeRow) error {
	if includeMeta {
		_, err := fmt.Fprintf(
			w,
			"%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
			row.target,
			row.from,
			row.fromK,
			row.fromType,
			formatMetric(row.fromSent),
			formatMetric(row.fromSentBytes),
			row.to,
			row.toK,
			row.toType,
			formatMetric(row.toReceived),
			formatMetric(row.toReceivedBytes),
		)
		return err
	}
	_, err := fmt.Fprintf(
		w,
		"%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
		row.from,
		row.fromK,
		row.fromType,
		formatMetric(row.fromSent),
		formatMetric(row.fromSentBytes),
		row.to,
		row.toK,
		row.toType,
		formatMetric(row.toReceived),
		formatMetric(row.toReceivedBytes),
	)
	return err
}

type topologyNode struct {
	id        string
	children  []string
	indegree  int
	recvBytes *float64
	sentBytes *float64
	recv      *float64
	sent      *float64
}

func renderTree(w io.Writer, includeMeta bool, listed []ListedTopology) error {
	groupOrder, grouped := groupTopologyItems(listed)

	for gi, groupKey := range groupOrder {
		items := grouped[groupKey]
		if err := writeTreeGroupHeader(w, includeMeta, items); err != nil {
			return err
		}

		nodes := buildTopologyNodes(items)
		roots := topologyRoots(nodes)

		visited := map[string]bool{}
		if err := writeTreeRoots(w, nodes, visited, roots); err != nil {
			return err
		}

		if err := writeTreeGroupSeparator(w, gi, len(groupOrder)); err != nil {
			return err
		}
	}
	return nil
}

func groupTopologyItems(listed []ListedTopology) ([]string, map[string][]ListedTopology) {
	groupOrder := make([]string, 0)
	grouped := map[string][]ListedTopology{}
	for _, item := range listed {
		groupKey := topologyGroupKey(item)
		if _, ok := grouped[groupKey]; !ok {
			groupOrder = append(groupOrder, groupKey)
		}
		grouped[groupKey] = append(grouped[groupKey], item)
	}
	return groupOrder, grouped
}

func writeTreeGroupHeader(w io.Writer, includeMeta bool, items []ListedTopology) error {
	if !includeMeta || len(items) == 0 {
		return nil
	}
	_, err := fmt.Fprintf(w, "TARGET %s\n", topologyTargetLabel(items[0]))
	return err
}

func buildTopologyNodes(items []ListedTopology) map[string]*topologyNode {
	nodes := map[string]*topologyNode{}
	for _, item := range items {
		n := ensureNode(nodes, item.ComponentID)
		n.children = mergeIDs(n.children, item.Children)
		n.recvBytes = item.ReceivedBytesTotal
		n.sentBytes = item.SentBytesTotal
		n.recv = item.ReceivedEventsTotal
		n.sent = item.SentEventsTotal
		for _, child := range item.Children {
			ensureNode(nodes, child).indegree++
		}
	}
	return nodes
}

func topologyRoots(nodes map[string]*topologyNode) []string {
	roots := make([]string, 0)
	for id, node := range nodes {
		if node.indegree == 0 {
			roots = append(roots, id)
		}
	}
	if len(roots) == 0 {
		for id := range nodes {
			roots = append(roots, id)
		}
	}
	sort.Strings(roots)
	return roots
}

func writeTreeRoots(w io.Writer, nodes map[string]*topologyNode, visited map[string]bool, roots []string) error {
	for i, root := range roots {
		if err := writeTreeNode(w, nodes, visited, root, "", i == len(roots)-1, false); err != nil {
			return err
		}
	}
	return nil
}

func writeTreeGroupSeparator(w io.Writer, groupIndex, groupCount int) error {
	if groupIndex >= groupCount-1 {
		return nil
	}
	_, err := fmt.Fprintln(w)
	return err
}

func ensureNode(nodes map[string]*topologyNode, id string) *topologyNode {
	if node, ok := nodes[id]; ok {
		return node
	}
	node := &topologyNode{id: id}
	nodes[id] = node
	return node
}

func nodeMetricSuffix(node *topologyNode) string {
	if node == nil || (node.recv == nil && node.sent == nil && node.recvBytes == nil && node.sentBytes == nil) {
		return ""
	}
	return fmt.Sprintf(
		" [recv_events=%s sent_events=%s recv_bytes=%s sent_bytes=%s]",
		formatMetric(node.recv),
		formatMetric(node.sent),
		formatMetric(node.recvBytes),
		formatMetric(node.sentBytes),
	)
}

func writeTreeNode(w io.Writer, nodes map[string]*topologyNode, visited map[string]bool, id, prefix string, isLast, withBranch bool) error {
	linePrefix := treeLinePrefix(prefix, isLast, withBranch)

	if visited[id] {
		_, err := fmt.Fprintf(w, "%s%s%s (shared)\n", linePrefix, id, nodeMetricSuffix(nodes[id]))
		return err
	}
	visited[id] = true

	if _, err := fmt.Fprintf(w, "%s%s%s\n", linePrefix, id, nodeMetricSuffix(nodes[id])); err != nil {
		return err
	}

	node, ok := nodes[id]
	if !ok || len(node.children) == 0 {
		return nil
	}

	children := append([]string{}, node.children...)
	sort.Strings(children)
	nextPrefix := treeNextPrefix(prefix, isLast, withBranch)
	for i, child := range children {
		if err := writeTreeNode(w, nodes, visited, child, nextPrefix, i == len(children)-1, true); err != nil {
			return err
		}
	}
	return nil
}

func treeLinePrefix(prefix string, isLast, withBranch bool) string {
	if !withBranch {
		return prefix
	}
	branch := "|- "
	if isLast {
		branch = "`- "
	}
	return prefix + branch
}

func treeNextPrefix(prefix string, isLast, withBranch bool) string {
	if !withBranch {
		return prefix
	}
	if isLast {
		return prefix + "   "
	}
	return prefix + "|  "
}

func topologyGroupKey(item ListedTopology) string {
	return fmt.Sprintf("%s/%s", item.Namespace, item.PodName)
}

func topologyTargetLabel(item ListedTopology) string {
	return fmt.Sprintf("%s/%s", item.Namespace, item.PodName)
}

//nolint:tagliatelle
type topologyView struct {
	TargetID            string   `json:"target_id,omitempty" yaml:"target_id,omitempty"`
	Namespace           string   `json:"namespace,omitempty" yaml:"namespace,omitempty"`
	PodName             string   `json:"pod_name,omitempty" yaml:"pod_name,omitempty"`
	ComponentID         string   `json:"component_id" yaml:"component_id"`
	ComponentKind       string   `json:"component_kind" yaml:"component_kind"`
	ComponentType       string   `json:"component_type" yaml:"component_type"`
	ReceivedBytesTotal  *float64 `json:"received_bytes_total,omitempty" yaml:"received_bytes_total,omitempty"`
	SentBytesTotal      *float64 `json:"sent_bytes_total,omitempty" yaml:"sent_bytes_total,omitempty"`
	ReceivedEventsTotal *float64 `json:"received_events_total,omitempty" yaml:"received_events_total,omitempty"`
	SentEventsTotal     *float64 `json:"sent_events_total,omitempty" yaml:"sent_events_total,omitempty"`
	Sources             []string `json:"sources,omitempty" yaml:"sources,omitempty"`
	Transforms          []string `json:"transforms,omitempty" yaml:"transforms,omitempty"`
	Sinks               []string `json:"sinks,omitempty" yaml:"sinks,omitempty"`
	Outputs             []string `json:"outputs,omitempty" yaml:"outputs,omitempty"`
	Parents             []string `json:"parents,omitempty" yaml:"parents,omitempty"`
	Children            []string `json:"children,omitempty" yaml:"children,omitempty"`
}

func stripMeta(in []ListedTopology, includeMeta bool) []topologyView {
	out := make([]topologyView, 0, len(in))
	for _, item := range in {
		v := topologyView{
			ComponentID:         item.ComponentID,
			ComponentKind:       item.ComponentKind,
			ComponentType:       item.ComponentType,
			ReceivedBytesTotal:  item.ReceivedBytesTotal,
			SentBytesTotal:      item.SentBytesTotal,
			ReceivedEventsTotal: item.ReceivedEventsTotal,
			SentEventsTotal:     item.SentEventsTotal,
			Sources:             item.Sources,
			Transforms:          item.Transforms,
			Sinks:               item.Sinks,
			Outputs:             item.Outputs,
			Parents:             item.Parents,
			Children:            item.Children,
		}
		if includeMeta {
			v.TargetID = item.TargetID
			v.Namespace = item.Namespace
			v.PodName = item.PodName
		}
		out = append(out, v)
	}
	return out
}

func directTarget(i int, endpointURL string) targets.Target {
	host := fmt.Sprintf("direct-%d", i+1)
	if u, err := url.Parse(endpointURL); err == nil && u.Host != "" {
		host = u.Host
	}
	safe := strings.NewReplacer("/", "_", ":", "_", ".", "_").Replace(host)
	return targets.Target{
		ID:        "direct/" + safe,
		Namespace: "direct",
		PodName:   host,
	}
}

func componentRefIDs(items []vectorapi.TopologyComponentRef) []string {
	out := make([]string, 0, len(items))
	for _, item := range items {
		if item.ComponentID != "" {
			out = append(out, item.ComponentID)
		}
	}
	return uniqueSorted(out)
}

func outputIDs(items []vectorapi.TopologyOutput) []string {
	out := make([]string, 0, len(items))
	for _, item := range items {
		if item.OutputID != "" {
			out = append(out, item.OutputID)
		}
	}
	return uniqueSorted(out)
}

func mergeIDs(a, b []string) []string {
	out := make([]string, 0, len(a)+len(b))
	out = append(out, a...)
	out = append(out, b...)
	return uniqueSorted(out)
}

func uniqueSorted(in []string) []string {
	if len(in) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(in))
	out := make([]string, 0, len(in))
	for _, item := range in {
		if item == "" {
			continue
		}
		if _, ok := seen[item]; ok {
			continue
		}
		seen[item] = struct{}{}
		out = append(out, item)
	}
	if len(out) == 0 {
		return nil
	}
	sort.Strings(out)
	return out
}

func onlyOrphaned(in []ListedTopology) []ListedTopology {
	out := make([]ListedTopology, 0, len(in))
	for _, item := range in {
		if len(item.Parents) == 0 && len(item.Children) == 0 {
			out = append(out, item)
		}
	}
	return out
}

func formatMetric(v *float64) string {
	if v == nil {
		return "-"
	}
	if *v == float64(int64(*v)) {
		return strconv.FormatInt(int64(*v), 10)
	}
	return strconv.FormatFloat(*v, 'f', -1, 64)
}
