package nodes_test

import (
	"testing"

	"christiangeorgelucas/blob-connector/axiom"
)

// testContext is a testing.T-backed axiom.Context for unit tests. Populate
// secretsMap with any secrets your node needs during the test, or
// revokedNames (ADR-156) to exercise a revoked secret via
// axiom.Secrets.Status. Shared by every *_test.go file in this package —
// axiom create node scaffolds one copy per file, which collides once more
// than one node's test file exists; this is the single surviving copy.
type testContext struct {
	t            *testing.T
	secretsMap   map[string]string
	revokedNames map[string]bool
}

func newTestContext(t *testing.T) *testContext {
	return &testContext{t: t, secretsMap: map[string]string{}, revokedNames: map[string]bool{}}
}

// testLogger forwards log output to testing.T so it is captured per-test.
type testLogger struct{ t *testing.T }

func (l *testLogger) Debug(msg string, args ...any) { l.t.Logf("DEBUG  %s %v", msg, args) }
func (l *testLogger) Info(msg string, args ...any)  { l.t.Logf("INFO   %s %v", msg, args) }
func (l *testLogger) Warn(msg string, args ...any)  { l.t.Logf("WARN   %s %v", msg, args) }
func (l *testLogger) Error(msg string, args ...any) { l.t.Logf("ERROR  %s %v", msg, args) }

// testSecrets is a simple in-memory axiom.Secrets backed by testContext.secretsMap.
type testSecrets struct {
	m       map[string]string
	revoked map[string]bool
}

func (s testSecrets) Get(name string) (string, bool) { v, ok := s.m[name]; return v, ok }

func (s testSecrets) Status(name string) axiom.SecretStatus {
	if _, ok := s.m[name]; ok {
		return axiom.SecretStatusAvailable
	}
	if s.revoked[name] {
		return axiom.SecretStatusRevoked
	}
	return axiom.SecretStatusUnset
}

// testFlowReflection is an empty running-flow view — no graph in a unit test.
type testFlowReflection struct{}

func (testFlowReflection) Nodes() []axiom.ReflectionNode     { return nil }
func (testFlowReflection) Edges() []axiom.ReflectionEdge     { return nil }
func (testFlowReflection) LoopEdges() []axiom.ReflectionEdge { return nil }
func (testFlowReflection) Position() axiom.FlowPosition      { return axiom.FlowPosition{} }
func (testFlowReflection) GraphID() string                   { return "" }

type testReflection struct{}

func (testReflection) Flow() axiom.FlowReflection { return testFlowReflection{} }

// testFlowMutation is a no-op mutation sink (this package has no
// mutation-capable nodes).
type testFlowMutation struct{}

func (testFlowMutation) AddNode(_, _, _ string, _ *axiom.CanvasPosition) uint32 { return 0 }
func (testFlowMutation) AddEdge(_, _ uint32, _ *axiom.EdgeCondition)            {}

type testMutation struct{}

func (testMutation) Flow() axiom.FlowMutation { return testFlowMutation{} }

func (c *testContext) Log() axiom.Logger            { return &testLogger{c.t} }
func (c *testContext) Secrets() axiom.Secrets       { return testSecrets{c.secretsMap, c.revokedNames} }
func (c *testContext) ExecutionID() string          { return "test-execution-id" }
func (c *testContext) FlowID() string               { return "test-flow-id" }
func (c *testContext) TenantID() string             { return "test-tenant-id" }
func (c *testContext) Reflection() axiom.Reflection { return testReflection{} }
func (c *testContext) Mutation() axiom.Mutation     { return testMutation{} }

var _ axiom.Context = (*testContext)(nil) // compile-time interface check
