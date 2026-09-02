# Agent execution profile

Buzz Desktop shows the effective execution settings for a managed agent in its
profile under **Runtime**.

The **Execution** section reports:

- **Runs on** — this computer or the selected remote server;
- **Harness** — the command that runs the agent harness;
- **Model provider** — the provider used for model requests;
- **Model** — the effective model identifier.

These values describe the managed-agent record after its configured and
inherited settings have been resolved. Optional values that are not reported
are left out rather than replaced with a guess. Remote provider configuration
and credentials are not shown in the profile.

## Editing the profile

Select **Edit** in the **Execution** section to open the existing agent
settings. Depending on the selected harness, its provider and model can be
changed there. The selected run location is chosen when an agent is created;
changing an existing agent from local to remote (or back) requires a separate
redeployment workflow.

This profile is informational and does not perform automatic model or
location routing. It gives the user a clear view of the settings that will be
used before changing them manually.
