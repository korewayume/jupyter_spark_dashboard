import * as React from 'react';
import lodash from 'lodash';
import {ISessionContext, ReactWidget, UseSignal} from "@jupyterlab/apputils"
// import {INotebookTracker} from "@jupyterlab/notebook";
import {NotebookPanel, Notebook} from "@jupyterlab/notebook";
import {Cell} from "@jupyterlab/cells";
import {Kernel, KernelMessage} from "@jupyterlab/services";
import {IComm} from "@jupyterlab/services/lib/kernel/kernel";
import {JSONObject} from "@lumino/coreutils";
import {each} from "@lumino/algorithm";
import {Signal} from "@lumino/signaling";
import {Message} from "@lumino/messaging";
import {Widget} from "@lumino/widgets/lib/widget";

interface ISparkTask {
    id: number;
    status: string;
}

class SparkStage {
    id: number;
    numTasks: number;
    name: string;
    tasks: Array<ISparkTask>;
    status: string;

    constructor(options: any) {
        this.id = options.id;
        this.numTasks = options.numTasks;
        this.name = options.name;
        this.tasks = [];
        this.status = options.status || "PENDING";
    }

    get numRunning() {
        return lodash.sum(this.tasks.map(task => task.status === 'RUNNING' ? 1 : 0))
    }

    get numSuccess() {
        return lodash.sum(this.tasks.map(task => task.status === 'SUCCESS' ? 1 : 0))
    }

}

class SparkJob {
    id: number;
    status: string;
    stages: Array<SparkStage>;

    constructor(options: any) {
        this.id = options.jobId;
        this.status = options.status;
        this.stages = [];
    }

    get numTasks() {
        return lodash.sum(lodash.map(this.stages, stage => stage.status !== "SKIPPED" ? stage.numTasks : 0))
    }

    get numSuccess() {
        return lodash.sum(lodash.map(this.stages, stage => stage.numSuccess))
    }

    get numRunning() {
        return lodash.sum(lodash.map(this.stages, stage => stage.numRunning))
    }

    get stageSummary() {
        const total = lodash.sum(this.stages.map(stage => stage.status !== "SKIPPED" ? 1 : 0));
        const skipped = this.stages.length - total;
        const completed = lodash.sum(this.stages.map(stage => stage.status === "COMPLETED" ? 1 : 0));
        let summary = `${completed}/${total}`;
        if (skipped>0) {
            summary = summary + ` (${skipped} skipped)`
        }
        return summary
    }

}

class SparkApplication {
    appId: string;
    appName: string;
    sparkUser: string;
    numCores: number;
    totalCores: number;
    jobs: Array<SparkJob>;
    stagesMap: Map<number, SparkStage>;
    tasksMap: Map<number, ISparkTask>;

    constructor(options: any) {
        this.appId = options.appId;
        this.appName = options.appName;
        this.sparkUser = options.sparkUser;
        this.numCores = options.numCores;
        this.totalCores = options.totalCores;
        this.jobs = [];
        this.stagesMap = new Map<number, SparkStage>();
        this.tasksMap = new Map<number, ISparkTask>();
    }

    get numCompleted() {
        return lodash.sum(this.jobs.map(job => job.status === 'SUCCEEDED' ? 1 : 0))
    }

    get numRunning() {
        return lodash.sum(this.jobs.map(job => job.status === 'RUNNING' ? 1 : 0))
    }

    update(options: any) {
        switch (options.msgtype) {
            case "sparkJobStart": {
                const job = new SparkJob(options);
                each(Object.entries(options.stageInfos) as Array<[string, any]>, (stageObj, index) => {
                    const [stageId, stageInfo] = stageObj;
                    stageInfo.id = Number(stageId);
                    const stage = new SparkStage(stageInfo);
                    job.stages.push(stage);
                    this.stagesMap.set(stage.id, stage)
                });
                this.jobs.push(job);
                break;
            }
            case "sparkStageSubmitted":
                break;
            case "sparkTaskStart": {
                const {taskId, status, stageId} = options;
                const task = {id: taskId, status};
                this.tasksMap.set(task.id, task);
                const stage = this.stagesMap.get(stageId);
                stage.status = "RUNNING";
                stage.tasks.push(task);
                break;
            }
            case "sparkTaskEnd": {
                const {taskId, status} = options;
                const task = this.tasksMap.get(taskId);
                task.status = status;
                break;
            }
            case "sparkStageCompleted": {
                const {stageId, status} = options;
                const stage = this.stagesMap.get(stageId);
                stage.status = status;
                break;
            }
            case "sparkJobEnd": {
                const {jobId, status} = options;
                const job = this.jobs.find(item => item.id === jobId);
                job.status = status;
                job.stages.forEach(stage => {
                    if (stage.status === "PENDING") {
                        stage.status = "SKIPPED"
                    }
                });
                break;
            }
        }
    }
}

export class SparkDashboardNotebook extends Notebook {
    sessionContext: ISessionContext;
    kernel: Kernel.IKernelConnection;
    comm: IComm;
    application: SparkApplication;
    readonly signal = new Signal<this, SparkApplication>(this);

    onAfterAttach(msg: Message) {
        super.onAfterAttach(msg);
        this.handleAttached();
    }

    handleAttached() {
        if (!this.sessionContext) {
            this.sessionContext = (this.parent as NotebookPanel).sessionContext;
            this.sessionContext.statusChanged.connect(this.handleKernelChanged, this)
        }
    }

    handleKernelChanged(sessionContext: ISessionContext, status: Kernel.Status) {
        const unavaliable_status = ['starting', 'restarting', 'unknown', 'autorestarting', 'dead'];
        if (unavaliable_status.includes(status)) {
            this.application = undefined;
            this.signal.emit(this.application);
        }
        const kernel = sessionContext.session?.kernel;
        if (kernel && this.kernel !== kernel) {
            this.kernel = sessionContext.session.kernel;
            this.kernel.registerCommTarget('spark-monitor', this.commTargetHandler.bind(this));
        }
    }

    commTargetHandler(comm: IComm, commMsg: KernelMessage.ICommOpenMsg) {
        if (commMsg.content.target_name !== 'spark-monitor') {
            return;
        }
        this.comm = comm;
        comm.onMsg = msg => {
            this.onMessage(msg.content.data)
        };
        comm.onClose = msg => {
            console.log("comm close", msg.content.data);
        };
    }

    onMessage(message: JSONObject) {
        if (!this.application) {
            this.application = new SparkApplication(message.application)
        }
        this.application.update(message);
        this.signal.emit(this.application);
    }
}

export class NotebookContentFactory extends NotebookPanel.ContentFactory {
    constructor(
        options?: Cell.ContentFactory.IOptions | undefined,
    ) {
        super(options);
    }

    createNotebook(options: Notebook.IOptions): Notebook {
        return new SparkDashboardNotebook(options)
    }
}


export class SparkDashboardWidget extends ReactWidget {
    constructor(notebook: SparkDashboardNotebook, options?: Widget.IOptions) {
        super(options);
        this.notebook = notebook;
    }

    notebook: SparkDashboardNotebook;

    render() {
        return <UseSignal signal={this.notebook.signal}>{
            (notebook: SparkDashboardNotebook, application: SparkApplication) => {
                return (
                    application ? <div className="spark-dashboard">
                        <div className="application">
                            <span>application</span>
                            <span>appId:</span><span>{application.appId}</span>
                            <span>appName:</span><span>{application.appName}</span>
                            <span>sparkUser:</span><span>{application.sparkUser}</span>
                            <span>totalCores:</span><span>{application.totalCores}</span>
                            <span>numCores:</span><span>{application.numCores}</span>
                            <span>{application.numRunning}</span><span>RUNNING</span>
                            <span>{application.numCompleted}</span><span>COMPLETED</span>
                        </div>
                        {
                            application.jobs.map(job => {
                                return (
                                    <div className="jobs" key={job.id}>
                                        <div className="job">
                                            <span>job</span>
                                            <span>jobId:</span><span>{job.id}</span>
                                            <span>status:</span><span>{job.status}</span>
                                            <span>stages:</span><span>{job.stageSummary}</span>
                                            <span>tasks:</span>
                                            <span>{job.numSuccess}</span>
                                            <span>+ {job.numRunning}</span>
                                            <span>/</span>
                                            <span>{job.numTasks}</span>
                                        </div>
                                        {
                                            job.stages.map(stage => {
                                                return (
                                                    <div className="stages" key={stage.id}>
                                                        <div className="stage">
                                                            <span>stage</span>
                                                            <span>stageId:</span><span>{stage.id}</span>
                                                            <span>name:</span><span>{stage.name.split(' ')[0]}</span>
                                                            <span>status:</span><span>{stage.status}</span>
                                                            <span>progress:</span>
                                                            <span>{stage.numSuccess}</span>
                                                            <span>+ {stage.numRunning}</span>
                                                            <span>/</span><span>{stage.numTasks}</span>
                                                        </div>
                                                    </div>
                                                )
                                            })
                                        }
                                    </div>
                                )
                            })
                        }
                    </div> : null
                )
            }
        }</UseSignal>
    }
}
