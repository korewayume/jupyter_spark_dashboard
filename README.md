# jupyter_spark_dashboard

A JupyterLab PySpark extension.

![](./docs/demo.gif)


## Requirements

* JupyterLab >= 2.0

## Install

```bash
jupyter labextension install jupyter_spark_dashboard
```

## Contributing

### Install

The `jlpm` command is JupyterLab's pinned version of
[yarn](https://yarnpkg.com/) that is installed with JupyterLab. You may use
`yarn` or `npm` in lieu of `jlpm` below.

```bash
# Clone the repo to your local environment
# Move to jupyter_spark_dashboard directory
# Install dependencies
jlpm
# Build Typescript source
jlpm build
# Link your development version of the extension with JupyterLab
jupyter labextension link .
# Rebuild Typescript source after making changes
jlpm build
# Rebuild JupyterLab after making any changes
jupyter lab build
# Install kernel extension
pip install .
# Enable kernel extension
ipython profile create && echo "c.InteractiveShellApp.extensions.append('jupyter_spark_monitor.kernel_extension')" >>  $(ipython profile locate default)/ipython_kernel_config.py
```

You can watch the source directory and run JupyterLab in watch mode to watch for changes in the extension's source and automatically rebuild the extension and application.

```bash
# Watch the source directory in another terminal tab
jlpm watch
# Run jupyterlab in watch mode in one terminal tab
jupyter lab --watch
```

### Uninstall

```bash
jupyter labextension uninstall jupyter_spark_dashboard
pip uninstall jupyter_spark_monitor
# Edit ipython kernel config
# $(ipython profile locate default)/ipython_kernel_config.py
```

# Acknowledgements
scala_listener copied from [krishnan-r/sparkmonitor](https://github.com/krishnan-r/sparkmonitor)

