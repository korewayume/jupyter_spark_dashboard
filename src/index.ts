import {
  JupyterFrontEnd, JupyterFrontEndPlugin
} from '@jupyterlab/application';


/**
 * Initialization data for the spark_dashboard extension.
 */
const extension: JupyterFrontEndPlugin<void> = {
  id: 'spark_dashboard',
  autoStart: true,
  activate: (app: JupyterFrontEnd) => {
    console.log('JupyterLab extension spark_dashboard is activated!');
  }
};

export default extension;
