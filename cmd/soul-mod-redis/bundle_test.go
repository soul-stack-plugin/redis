// Guards on the artifact itself, as opposed to on what a module does.
//
// They run in `go test`, before anything is built or stamped, because all three
// failures below are cheap here and expensive later: an invalid bundle stops
// `soul-mod stamp` at build time, and a bundle that dispatches loosely reaches a
// host and changes the wrong thing.
package main

import (
	"bytes"
	"testing"

	"github.com/souls-guild/soul-stack/sdk/module"
	"github.com/souls-guild/soul-stack/sdk/schema"
)

func TestBundleIsValid(t *testing.T) {
	for _, issue := range bundle.Validate() {
		t.Errorf("%s %s %s: %s", issue.Level, issue.Path, issue.Code, issue.Message)
	}
}

// The schema subcommand is the only source `soul-mod stamp` reads, and it stores
// what it gets verbatim. So what this artifact prints has to be a document the
// SDK itself would have produced — parseable, valid, and canonical byte for
// byte. Checking it here means a bad Def fails the test run rather than the
// build step after it.
func TestSchemaSubcommandPrintsACanonicalDocument(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if code := module.ServeBundleArgs(bundle, []string{schema.SchemaSubcommand}, &stdout, &stderr); code != 0 {
		t.Fatalf("schema subcommand exited %d: %s", code, stderr.String())
	}
	doc, err := schema.Unmarshal(stdout.Bytes())
	if err != nil {
		t.Fatalf("the printed document does not parse: %v", err)
	}
	for _, issue := range schema.Validate(doc) {
		t.Errorf("%s %s %s: %s", issue.Level, issue.Path, issue.Code, issue.Message)
	}
	canonical, err := schema.IsCanonical(stdout.Bytes())
	if err != nil {
		t.Fatalf("canonical check: %v", err)
	}
	if !canonical {
		t.Fatal("the printed document is not canonical, so `soul-mod stamp` will refuse it")
	}
}

// Which module runs decides which host gets changed, so an artifact asked for a
// module it does not serve must refuse — never fall through to the only one it
// has. `redis` is the argument to guard against by name: it is the registration
// alias an operator sees everywhere, and it is not a module.
func TestUnknownModuleDoesNotFallThroughToTheOnlyOne(t *testing.T) {
	for _, name := range []string{"redis", "config", "info", "present", ""} {
		var stdout, stderr bytes.Buffer
		var args []string
		if name != "" {
			args = []string{name}
		}
		if code := module.ServeBundleArgs(bundle, args, &stdout, &stderr); code == 0 {
			t.Errorf("argv %q was accepted; this artifact serves only the modules it declares", name)
		}
		if stdout.Len() != 0 {
			t.Errorf("argv %q wrote to stdout: %q", name, stdout.String())
		}
	}
}
