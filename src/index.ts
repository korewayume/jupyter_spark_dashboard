import {JupyterFrontEnd, JupyterFrontEndPlugin} from '@jupyterlab/application';
import {MainAreaWidget} from '@jupyterlab/apputils';
import {NotebookPanel, INotebookTracker, INotebookModel} from "@jupyterlab/notebook";
import {SparkDashboardWidget, NotebookContentFactory, SparkDashboardNotebook} from "./widget";
import {IEditorServices} from '@jupyterlab/codeeditor';
import {IMainMenu} from '@jupyterlab/mainmenu';
import {ToolbarButton} from '@jupyterlab/apputils';
import {stopIcon} from '@jupyterlab/ui-components';

import {
    IDisposable, DisposableDelegate
} from '@lumino/disposable';
import {
    DocumentRegistry
} from '@jupyterlab/docregistry';
import {Kernel} from "@jupyterlab/services/lib/kernel";


const notebookFactory: JupyterFrontEndPlugin<NotebookPanel.IContentFactory> = {
    id: 'spark-dashboard-notebook:factory',
    provides: NotebookPanel.IContentFactory,
    requires: [IEditorServices],
    autoStart: true,
    activate: (app: JupyterFrontEnd, editorServices: IEditorServices) => {
        console.log('app', app);
        let editorFactory = editorServices.factoryService.newInlineEditor;
        return new NotebookContentFactory({editorFactory});
    }
};

class ButtonExtension implements DocumentRegistry.IWidgetExtension<NotebookPanel, INotebookModel> {
    createNew(panel: NotebookPanel, context: DocumentRegistry.IContext<INotebookModel>): IDisposable {
        const pysparkCancelAllJobs = async () => {
            async function waitKernelStatusOK(kernel: Kernel.IKernelConnection) {
                try{
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
            icon: stopIcon,
            onClick: pysparkCancelAllJobs,
            tooltip: 'Interrupt the kernel & Cancel All PySpark Jobs'
        });

        panel.toolbar.insertItem(9,'Interrupt the kernel & Cancel All PySpark Jobs', button);
        return new DisposableDelegate(() => {
            button.dispose();
        });
    }
}

const dashboardCommand: JupyterFrontEndPlugin<void> = {
    id: 'spark_dashboard',
    requires: [INotebookTracker, IMainMenu],
    autoStart: true,
    activate: (app: JupyterFrontEnd, tracker: INotebookTracker, mainMenu: IMainMenu) => {
        const openCommand: string = 'spark_dashboard:open';
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
                content.addClass('spark_dashboard');
                const widget = new MainAreaWidget({content});
                widget.id = 'spark_dashboard';
                widget.title.label = 'Spark Dashboard';
                widget.title.closable = true;
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
