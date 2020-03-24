import {JupyterFrontEnd, JupyterFrontEndPlugin} from '@jupyterlab/application';
import {MainAreaWidget} from '@jupyterlab/apputils';
import { IDocumentManager } from '@jupyterlab/docmanager';
import {NotebookPanel, INotebookTracker} from "@jupyterlab/notebook"
import {SparkDashboardWidget, NotebookContentFactory, SparkDashboardNotebook} from "./widget";
import {IEditorServices} from '@jupyterlab/codeeditor';

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


const dashboardCommand: JupyterFrontEndPlugin<void> = {
    id: 'spark_dashboard',
    requires: [ INotebookTracker, IDocumentManager],
    autoStart: true,
    activate: (app: JupyterFrontEnd, tracker: INotebookTracker, docManager: IDocumentManager) => {

        // Add an application command
        const command: string = 'spark_dashboard:open';
        app.commands.addCommand(command, {
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
            command: command,
            selector: '.jp-Notebook',
        });
    }
};

const extensions: JupyterFrontEndPlugin<any>[] = [
    notebookFactory, dashboardCommand
];

export default extensions;
