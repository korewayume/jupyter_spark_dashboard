import {
  IFileBrowserFactory
} from '@jupyterlab/filebrowser';

import {
  JupyterFrontEnd, JupyterFrontEndPlugin
} from '@jupyterlab/application';

import {
  Clipboard,
} from '@jupyterlab/apputils';

import {
  linkIcon,
} from '@jupyterlab/ui-components';

import {URLExt} from '@jupyterlab/coreutils';

import {ServerConnection} from '@jupyterlab/services';

import {toArray} from '@lumino/algorithm';


export const shareFile: JupyterFrontEndPlugin<void> = {
  activate: activateShareFile,
  id: 'share-file-beta',
  requires: [IFileBrowserFactory],
  autoStart: true
};

function activateShareFile(
  app: JupyterFrontEnd,
  factory: IFileBrowserFactory
): void {
  const { commands } = app;
  const { tracker } = factory;

  commands.addCommand('filebrowser:share-main', {
    execute: async () => {
      const widget = tracker.currentWidget;
      const model = widget?.selectedItems().next();
      if (!model) {
        return;
      }
      const path = encodeURI(model.path);
      const settings = ServerConnection.makeSettings();
      let url = URLExt.join(settings.baseUrl, 'api/share_notebook');
      let init = {method: 'POST', body: JSON.stringify({path: path})};
      let response = await ServerConnection.makeRequest(url, init, settings);
      let jsonBody = await response.json();
      console.log(path, jsonBody);
      Clipboard.copyToSystem(URLExt.normalize(jsonBody.url));
    },
    isVisible: () =>
      !!tracker.currentWidget &&
      toArray(tracker.currentWidget.selectedItems()).length === 1,
    icon: linkIcon.bindprops({ stylesheet: 'menuItem' }),
    label: 'Copy Shareable Link (beta)'
  });
}
