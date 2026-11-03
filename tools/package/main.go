// Command package builds connectors for each platform and writes their
// descriptors with `openetl connector package`, for a release's assets.
//
//	go run ./tools/package --version 0.1.0 [connector ...]
package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strings"

	"gopkg.in/yaml.v3"
)

type metadata struct {
	Author string `yaml:"author"`
	Logo   string `yaml:"logo"`
}

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "package:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	fs := flag.NewFlagSet("package", flag.ContinueOnError)
	version := fs.String("version", "", "the connectors' version, such as 0.1.0")
	tag := fs.String("tag", "v{version}", "the release's git tag, with {connector} and {version} in it")
	repo := fs.String("repo", "https://github.com/unsuitable001/openetl-connectors", "the repository the release is in")
	platforms := fs.String("platforms", "linux/amd64,linux/arm64,darwin/amd64,darwin/arm64", "the os/arch pairs to build")
	out := fs.String("out", "dist", "where to write the executables and descriptors")
	openetl := fs.String("openetl", defaultCLI(), "the openetl CLI")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *version == "" {
		return errors.New("--version is required")
	}
	b, err := os.ReadFile("connectors.yaml")
	if err != nil {
		return err
	}
	var all map[string]metadata
	if err := yaml.Unmarshal(b, &all); err != nil {
		return fmt.Errorf("connectors.yaml: %w", err)
	}
	names := fs.Args()
	if len(names) == 0 {
		for n := range all {
			names = append(names, n)
		}
		slices.Sort(names)
	}
	plats := strings.Split(*platforms, ",")
	if host := runtime.GOOS + "/" + runtime.GOARCH; !slices.Contains(plats, host) {
		return fmt.Errorf("--platforms must include %s, whose executable is run to read the spec", host)
	}
	if err := os.MkdirAll(*out, 0o755); err != nil {
		return err
	}
	for _, n := range names {
		m, ok := all[n]
		if !ok {
			return fmt.Errorf("%s isn't in connectors.yaml", n)
		}
		kind, name, err := split(n)
		if err != nil {
			return err
		}
		t := strings.NewReplacer("{connector}", n, "{version}", *version).Replace(*tag)
		pkg := []string{"connector", "package",
			"--name", name, "--version", *version,
			"--url-template", *repo + "/releases/download/" + t + "/" + n + "-{os}-{arch}",
			"--author", m.Author, "--logo", m.Logo,
			"--release-tag", t, "--release-url", *repo + "/releases/tag/" + t,
			"-o", filepath.Join(*out, n+".yaml"),
		}
		for _, p := range plats {
			goos, arch, _ := strings.Cut(p, "/")
			exe := filepath.Join(*out, n+"-"+goos+"-"+arch)
			if err := command(append(os.Environ(), "GOOS="+goos, "GOARCH="+arch, "CGO_ENABLED=0"), "go", "build", "-o", exe, "./cmd/"+n); err != nil {
				return fmt.Errorf("build %s for %s: %w", n, p, err)
			}
			pkg = append(pkg, "--exe", p+"="+exe)
		}
		if err := command(os.Environ(), *openetl, pkg...); err != nil {
			return fmt.Errorf("package %s: %w", n, err)
		}
		doc, err := os.ReadFile(filepath.Join(*out, n+".yaml"))
		if err != nil {
			return err
		}
		if err := check(doc, kind, name); err != nil {
			return fmt.Errorf("%s: %w", n, err)
		}
		fmt.Printf("%s %s: %s\n", n, *version, filepath.Join(*out, n+".yaml"))
	}
	return nil
}

// split turns source-postgres into source and postgres: the platform names a
// connector by its kind and name, and its executable {kind}-{name}.
func split(connector string) (kind, name string, err error) {
	kind, name, _ = strings.Cut(connector, "-")
	if (kind != "source" && kind != "destination") || name == "" {
		return "", "", fmt.Errorf("%s isn't source-NAME or destination-NAME", connector)
	}
	return kind, name, nil
}

// check that the descriptor names the connector, with the kind its executable
// reported, so connectors.yaml's entry and the published version agree.
func check(doc []byte, kind, name string) error {
	var d struct {
		Metadata struct {
			Name string `yaml:"name"`
		} `yaml:"metadata"`
		Spec struct {
			Kind string `yaml:"kind"`
		} `yaml:"spec"`
	}
	if err := yaml.Unmarshal(doc, &d); err != nil {
		return err
	}
	if d.Spec.Kind != kind {
		return fmt.Errorf("the executable is a %s connector, not a %s", d.Spec.Kind, kind)
	}
	if d.Metadata.Name != name {
		return fmt.Errorf("the descriptor names %s, not %s", d.Metadata.Name, name)
	}
	return nil
}

func command(env []string, name string, args ...string) error {
	c := exec.Command(name, args...)
	c.Env, c.Stdout, c.Stderr = env, os.Stderr, os.Stderr
	return c.Run()
}

func defaultCLI() string {
	if p := filepath.Join("..", "openetl", "bin", "openetl"); fileExists(p) {
		return p
	}
	return "openetl"
}

func fileExists(p string) bool {
	_, err := os.Stat(p)
	return err == nil
}
