import {DisposableDelegate, IDisposable} from '@lumino/disposable';
import {DocumentRegistry} from '@jupyterlab/docregistry';
import {IEditorServices} from '@jupyterlab/codeeditor';
import {IMainMenu} from '@jupyterlab/mainmenu';
import {INotebookModel, INotebookTracker, NotebookPanel} from '@jupyterlab/notebook';
import {JupyterFrontEnd, JupyterFrontEndPlugin} from '@jupyterlab/application';
import {Kernel} from '@jupyterlab/services/lib/kernel';
import {MainAreaWidget} from '@jupyterlab/apputils';
import {
    NotebookContentFactory,
    SparkDashboardNotebook,
    SparkDashboardWidget,
    SparkIcon
} from './widget';
import {ToolbarButton} from '@jupyterlab/apputils';


const notebookFactory: JupyterFrontEndPlugin<NotebookPanel.IContentFactory> = {
    id: 'spark-dashboard-notebook:factory',
    provides: NotebookPanel.IContentFactory,
    requires: [IEditorServices],
    autoStart: true,
    activate: (app: JupyterFrontEnd, editorServices: IEditorServices) => {
        let editorFactory = editorServices.factoryService.newInlineEditor;
        return new NotebookContentFactory({editorFactory});
    }
};

class ButtonExtension implements DocumentRegistry.IWidgetExtension<NotebookPanel, INotebookModel> {
    createNew(panel: NotebookPanel, context: DocumentRegistry.IContext<INotebookModel>): IDisposable {
        const pysparkCancelAllJobs = async () => {
            async function waitKernelStatusOK(kernel: Kernel.IKernelConnection) {
                try {
                    await kernel.requestKernelInfo();
                } catch (error) {
                    if (error.message === 'Kernel info reply errored') {
                        await waitKernelStatusOK(kernel)
                    } else {
                        throw error;
                    }
                }
            }

            const kernel = context.sessionContext.session?.kernel;
            if (kernel) {
                await kernel.interrupt();
                await waitKernelStatusOK(kernel);
                await kernel.requestExecute({
                    silent: true,
                    store_history: false,
                    allow_stdin: false,
                    code: `
def pysparkCancelAllJobs():
    from pyspark.sql import SparkSession
    if SparkSession._instantiatedSession and SparkSession._instantiatedSession._sc:
        SparkSession._instantiatedSession._sc.cancelAllJobs()

pysparkCancelAllJobs()
`
                }, false).done;
            }
        };
        let button = new ToolbarButton({
            className: 'pyspark-cancel-all-jobs',
            icon: {
                name: 'spark-stop-icon',
                svgstr: `<svg viewBox="0 0 1024 1024" version="1.1" xmlns="http://www.w3.org/2000/svg"><path d="M512 0C229.233 0 0 229.233 0 512s229.233 512 512 512 512-229.233 512-512S794.767 0 512 0z m0 928.01C282.255 928.01 95.99 741.765 95.99 512S282.235 95.99 512 95.99 928.01 282.235 928.01 512 741.765 928.01 512 928.01zM320 320h384v384H320z" fill="#616161"></path></svg>`
            },
            onClick: pysparkCancelAllJobs,
            tooltip: 'Interrupt the kernel & Cancel All PySpark Jobs'
        });

        panel.toolbar.insertItem(9, 'Interrupt the kernel & Cancel All PySpark Jobs', button);
        return new DisposableDelegate(() => {
            button.dispose();
        });
    }
}

const dashboardCommand: JupyterFrontEndPlugin<void> = {
    id: 'jupyter_spark_dashboard',
    requires: [INotebookTracker, IMainMenu],
    autoStart: true,
    activate: (app: JupyterFrontEnd, tracker: INotebookTracker, mainMenu: IMainMenu) => {
        const openCommand: string = 'jupyter_spark_dashboard:open';
        app.commands.addCommand(openCommand, {
            label: 'Open Spark Dashboard',
            execute: () => {
                function getCurrent(): NotebookPanel | null {
                    const widget = tracker.currentWidget;
                    app.shell.activateById(widget.id);
                    return widget;
                }

                const current = getCurrent();
                const content = new SparkDashboardWidget(current.content as SparkDashboardNotebook);
                content.addClass('jupyter_spark_dashboard');
                const widget = new MainAreaWidget({content});
                widget.id = 'jupyter_spark_dashboard';
                widget.title.label = 'Spark Dashboard';
                widget.title.closable = true;
                widget.title.icon = new SparkIcon();
                current.context.addSibling(widget, {
                    ref: current.id,
                    mode: 'split-bottom'
                });
            }
        });
        app.contextMenu.addItem({
            command: openCommand,
            selector: '.jp-Notebook',
        });
        app.docRegistry.addWidgetExtension('Notebook', new ButtonExtension());
    }
};

const extensions: JupyterFrontEndPlugin<any>[] = [
    notebookFactory, dashboardCommand
];

export default extensions;
