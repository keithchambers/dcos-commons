package main

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"mime/multipart"
	"os"
	"path/filepath"

	"github.com/mesosphere/dcos-commons/cli"
	"github.com/mesosphere/dcos-commons/cli/client"
	"github.com/mesosphere/dcos-commons/cli/commands"
	"github.com/mesosphere/dcos-commons/cli/queries"
	"gopkg.in/alecthomas/kingpin.v3-unstable"
)

type runFlag struct {
	name string
}

func (r *runFlag) getPrefix() string {
	if len(r.name) == 0 {
		// Give better info than the default error.
		// Also, ensure that the arg can be visible to all 'run' commands, but is only 'required' for the ones that use it.
		fmt.Printf("Missing required '--run' argument or 'RUN_NAME' envvar")
		os.Exit(1)
	}
	return fmt.Sprintf("v1/run/%s/", r.name)
}

func addRunFlag(name *runFlag, cmd *kingpin.CmdClause) *kingpin.CmdClause {
	return cmd
}

func main() {
	app := cli.New()

	// Add the --run flag at the app level so that it can be specified in any position within the command.
	// We mark it optional here, since it's only used by certain commands. The requirement enforcement
	// is done in runFlag.getPrefix().
	name := runFlag{}
	app.Flag("run", "The active Run to query").Envar("RUN_NAME").PlaceHolder("RUN_NAME").StringVar(&name.name)

	configQueries := queries.NewConfig()
	configQueries.PrefixCb = name.getPrefix
	endpointsQueries := queries.NewEndpoints()
	endpointsQueries.PrefixCb = name.getPrefix
	pkgQueries := queries.NewPackage()
	// no PrefixCb for pkgQueries: These queries go to cosmos
	planQueries := queries.NewPlan()
	planQueries.PrefixCb = name.getPrefix
	podQueries := queries.NewPod()
	podQueries.PrefixCb = name.getPrefix
	stateQueries := queries.NewState()
	stateQueries.PrefixCb = name.getPrefix

	// Cosmos commands, no change for queues:
	commands.HandleDescribeSection(app, pkgQueries)
	commands.HandleUpdateSection(app, pkgQueries, planQueries)

	// - Adding/listing/removing runs
	// - Invoking HTTP calls against runs (pods, plans, ...)
	run := app.Command("run", "Run management").Alias("runs")

	// Commands mirroring the originals, except under a custom '/v1/run/RUNNAME/' prefix:
	debug := run.Command("debug", "View service state useful in debugging")
	commands.HandleDebugCommands(debug, configQueries, podQueries, stateQueries)

	endpoints := run.Command("endpoints", "View client endpoints").Alias("endpoint")
	commands.HandleEndpointsCommands(endpoints, endpointsQueries)

	plan := run.Command("plan", "Query service plans")
	commands.HandlePlanCommands(plan, planQueries)

	pod := run.Command("pod", "View Pod/Task state")
	commands.HandlePodCommands(pod, podQueries)

	// Commands supported by the queue itself:
	handleRunCommands(run)

	kingpin.MustParse(app.Parse(cli.GetArguments()))
}

type runHandler struct {
	run string
	specType string
	specFile string
}

func (cmd *runHandler) list(a *kingpin.Application, e *kingpin.ParseElement, c *kingpin.ParseContext) error {
	responseBytes, err := client.HTTPServiceGet("v1/runs")
	if err != nil {
		return err
	}
	client.PrintJSONBytes(responseBytes)
	return nil
}

func (cmd *runHandler) add(a *kingpin.Application, e *kingpin.ParseElement, c *kingpin.ParseContext) error {
	var fileBytes []byte
	var err error
	if cmd.specFile == "stdin" {
		// Read from stdin
		client.PrintMessage("Reading spec file from stdin...")
		fileBytes, err = ioutil.ReadAll(os.Stdin)
		if err != nil {
			return fmt.Errorf("Failed to read run spec from stdin: %s", err)
		}
	} else {
		// Read from file
		fileBytes, err = ioutil.ReadFile(cmd.specFile)
		if err != nil {
			return fmt.Errorf("Failed to read specified run spec file %s: %s", cmd.specFile, err)
		}
	}

	payloadBuf := &bytes.Buffer{}

	// Build multipart payload containing specified type and data:
	formWriter := multipart.NewWriter(payloadBuf)
	formWriter.WriteField("type", cmd.specType)
	fileWriter, err := formWriter.CreateFormFile("file", filepath.Base(cmd.specFile))
	if err != nil {
		return fmt.Errorf("Failed to create form: %s", err)
	}
	_, err = fileWriter.Write(fileBytes)
	if err != nil {
		return fmt.Errorf("Failed to store form data: %s", err)
	}
	err = formWriter.Close()
	if err != nil {
		return fmt.Errorf("Failed to write form data: %s", err)
	}

	responseBytes, err := client.HTTPServicePostData("v1/runs", payloadBuf.Bytes(), formWriter.FormDataContentType())
	if err != nil {
		return err
	}
	client.PrintJSONBytes(responseBytes)
	return nil
}

func (cmd *runHandler) delete(a *kingpin.Application, e *kingpin.ParseElement, c *kingpin.ParseContext) error {
	responseBytes, err := client.HTTPServiceDelete(fmt.Sprintf("v1/runs/%s", cmd.run))
	if err != nil {
		return err
	}
	client.PrintJSONBytes(responseBytes)
	return nil
}

func handleRunCommands(run *kingpin.CmdClause) {
	cmd := &runHandler{}

	run.Command("list", "Lists all active runs in the queue").Action(cmd.list)

	add := run.Command("add", "Lists all active runs in the queue").Action(cmd.add)
	add.Arg("type", "Type of run").Required().StringVar(&cmd.specType)
	add.Arg("path", "Path to run spec file, or 'stdin' to read from stdin").Required().StringVar(&cmd.specFile)

	delete := run.Command("remove", "Uninstalls an active run from the queue").Action(cmd.delete)
	delete.Arg("name", "name of run to delete").Required().StringVar(&cmd.run)
}
