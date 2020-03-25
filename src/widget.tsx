import * as React from 'react';
import lodash from 'lodash';
import moment from 'moment'
import 'moment-duration-format'
import {ISessionContext, ReactWidget, UseSignal} from "@jupyterlab/apputils"
import {NotebookPanel, Notebook} from "@jupyterlab/notebook";
import {Cell} from "@jupyterlab/cells";
import {Kernel, KernelMessage} from "@jupyterlab/services";
import {IComm} from "@jupyterlab/services/lib/kernel/kernel";
import {JSONObject} from "@lumino/coreutils";
import {each} from "@lumino/algorithm";
import {Signal} from "@lumino/signaling";
import {Message} from "@lumino/messaging";
import {Widget} from "@lumino/widgets/lib/widget";
import TimeAgo from 'timeago-react';

moment.locale('zh-cn');

interface PropsWithData<T> {
    data: T
}

interface PSWithItems<T> {
    items: T[]
}

interface ITaskProgressBarData {
    numSuccess: number;
    numRunning: number;
    numTasks: number;
}

interface ISparkStatusBarData {
    status: string;
    statusText: string;
}

interface ISparkTask {
    id: number;
    status: string;
}

class TaskProgressBar extends React.Component<PropsWithData<ITaskProgressBarData>, ITaskProgressBarData> {
    constructor(props: PropsWithData<ITaskProgressBarData>) {
        super(props);
        this.state = props.data;
    }

    get running() {
        return {width: `${(this.state.numRunning / this.state.numTasks) * 100}%`}
    }

    get success() {
        return {width: `${(this.state.numSuccess / this.state.numTasks) * 100}%`}
    }

    get text() {
        const success = `${this.state.numSuccess}`;
        const running = this.state.numRunning > 0 ? ` + ${this.state.numRunning}` : '';
        return `${success}${running} / ${this.state.numTasks}`
    }

    render() {
        return (
            <div className="spark-task-progress">
                <div className="task-progress-text">{this.text}</div>
                <span className="task-progress-success" style={this.success} />
                <span className="task-progress-running" style={this.running} />
            </div>
        )
    }
}

class SparkStatusBar extends React.Component<ISparkStatusBarData, ISparkStatusBarData> {
    constructor(props: ISparkStatusBarData) {
        super(props);
        this.state = {
            status: props.status,
            statusText: props.statusText
        };
    }

    componentWillReceiveProps(nextProps: ISparkStatusBarData) {
        this.setState({
            status: nextProps.status,
            statusText: nextProps.statusText
        })
    }

    get className() {
        return [this.state.status, 'spark-status-bar'].join(' ')
    }

    render() {
        return (
            <div className={this.className}>{this.state.statusText}</div>
        )
    }
}

interface ISubmissionTime {
    submissionTime: number;
}

interface IDuration extends ISubmissionTime {
    completionTime: number;
}

class SubmissionTime extends React.Component<PropsWithData<ISubmissionTime>, ISubmissionTime> {
    constructor(props: PropsWithData<ISubmissionTime>) {
        super(props);
        this.state = props.data;
    }

    componentWillReceiveProps(nextProps: PropsWithData<ISubmissionTime>) {
        this.setState({
            submissionTime: nextProps.data.submissionTime
        })
    }

    render() {
        return (
            this.state.submissionTime > 0 ? <TimeAgo locale='zh_CN' datetime={new Date(this.state.submissionTime)} /> :
                <span>未知</span>
        )
    }
}

class Duration extends React.Component<PropsWithData<IDuration>, IDuration> {
    constructor(props: PropsWithData<IDuration>) {
        super(props);
        this.state = props.data;
    }

    componentWillReceiveProps(nextProps: PropsWithData<IDuration>) {
        this.setState({
            submissionTime: nextProps.data.submissionTime,
            completionTime: nextProps.data.completionTime
        })
    }

    render() {
        return (
            <span>
                {
                    this.state.completionTime - this.state.submissionTime > 0 ?
                        (moment.duration(this.state.completionTime - this.state.submissionTime) as any).format("d[d] h[h]:mm[m]:ss[s]") : '未知'
                }
            </span>
        )
    }
}

interface SparkJobItemState {
    job: SparkJob,
    isCollapse: boolean
}

class SparkJobItem extends React.Component<PropsWithData<SparkJob>, SparkJobItemState> {
    constructor(props: PropsWithData<SparkJob>) {
        super(props);
        this.state = {job: props.data, isCollapse: true};
    }

    toggleCollapse() {
        this.setState({
            isCollapse: !this.state.isCollapse
        })
    }

    render() {
        return [
            <tr key={`job-${this.state.job.id}`}>
                <td className={'collapse'} onClick={this.toggleCollapse.bind(this)} />
                <td>{this.state.job.id}</td>
                <td><SparkStatusBar status={this.state.job.status} statusText={this.state.job.status} /></td>
                <td>{this.state.job.stageSummary}</td>
                <td><TaskProgressBar data={this.state.job} /></td>
                <td><SubmissionTime data={this.state.job} /></td>
                <td><Duration data={this.state.job} /></td>
            </tr>,
            this.state.isCollapse ? null : <tr key={`job-${this.state.job.id}-stages`}>
                <td className={'stage-collapse-offset'} />
                <td className={'stage-table-td'} colSpan={6}>
                    <SparkStageTable items={this.state.job.stages} />
                </td>
            </tr>
        ]
    }
}

class SparkJobTable extends React.Component<PSWithItems<SparkJob>, PSWithItems<SparkJob>> {
    constructor(props: PSWithItems<SparkJob>) {
        super(props);
        this.state = {items: props.items};
    }

    render() {
        return (
            <table>
                <thead>
                <tr>
                    <td className={'collapse-offset'} />
                    <td className={'width-10'}>Job ID</td>
                    <td className={'width-15'}>Status</td>
                    <td className={'width-15'}>Stages</td>
                    <td>Tasks</td>
                    <td className={'width-15'}>Submission Time</td>
                    <td className={'width-15'}>Duration</td>
                </tr>
                </thead>
                <tbody>
                {lodash.map(this.state.items, job => <SparkJobItem data={job}/>)}
                </tbody>
            </table>
        )
    }
}

class SparkStageTable extends React.Component<PSWithItems<SparkStage>, PSWithItems<SparkStage>> {
    constructor(props: PSWithItems<SparkStage>) {
        super(props);
        this.state = {items: props.items};
    }

    renderSparkStage(stage: SparkStage) {
        return (
            <tr key={`stage-${stage.id}`}>
                <td><span>{stage.id}</span></td>
                <td><span>{stage.name.split(' ')[0]}</span></td>
                <td><SparkStatusBar status={stage.status} statusText={stage.status} /></td>
                <td><TaskProgressBar data={stage} /></td>
                <td><SubmissionTime data={stage} /></td>
                <td><Duration data={stage} /></td>
            </tr>
        )
    }

    render() {
        return (
            <table>
                <thead>
                <tr>
                    <td className={'width-10'}>Stage ID</td>
                    <td className={'width-15'}>Stage Name</td>
                    <td className={'width-15'}>Status</td>
                    <td>Progress</td>
                    <td className={'width-15'}>Submission Time</td>
                    <td className={'width-15'}>Duration</td>
                </tr>
                </thead>
                <tbody>
                {lodash.map(this.state.items, this.renderSparkStage.bind(this))}
                </tbody>
            </table>
        )
    }
}

class SparkStage {
    id: number;
    numTasks: number;
    name: string;
    tasks: Array<ISparkTask>;
    status: string;
    submissionTime: number;
    completionTime: number;

    constructor(options: any) {
        this.id = options.id;
        this.numTasks = options.numTasks;
        this.name = options.name;
        this.tasks = [];
        this.status = options.status || "PENDING";
        this.submissionTime = options.submissionTime > 0 ? options.submissionTime : -1;
        this.completionTime = options.completionTime > 0 ? options.completionTime : -1;
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
    submissionTime: number;
    completionTime: number;

    constructor(options: any) {
        this.id = options.jobId;
        this.status = options.status;
        this.stages = [];
        this.submissionTime = options.submissionTime > 0 ? options.submissionTime : -1;
        this.completionTime = options.completionTime > 0 ? options.completionTime : -1;
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
        if (skipped > 0) {
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
                each(Object.entries(options.stageInfos) as Array<[string, any]>, (stageObj) => {
                    const [stageId, stageInfo] = stageObj;
                    stageInfo.id = Number(stageId);
                    const stage = new SparkStage(stageInfo);
                    job.stages.push(stage);
                    this.stagesMap.set(stage.id, stage)
                });
                this.jobs.push(job);
                break;
            }
            case "sparkStageSubmitted": {
                const {stageId, submissionTime} = options;
                const stage = this.stagesMap.get(stageId);
                stage.submissionTime = submissionTime;
                break;
            }
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
                const {stageId, status, completionTime} = options;
                const stage = this.stagesMap.get(stageId);
                stage.status = status;
                stage.completionTime = completionTime;
                break;
            }
            case "sparkJobEnd": {
                const {jobId, status, completionTime} = options;
                const job = this.jobs.find(item => item.id === jobId);
                job.status = status;
                job.completionTime = completionTime;
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
        const unavailable_status = ['starting', 'restarting', 'unknown', 'autorestarting', 'dead'];
        if (unavailable_status.includes(status)) {
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
        console.log(message, this.application);
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
                            <span className={'spark-dashboard-application-item'}>{application.appId}</span>
                            <span className={'spark-dashboard-application-item'}>{application.appName}</span>
                            <span className={'spark-dashboard-application-item'}>{application.sparkUser}</span>
                            <span className={'spark-dashboard-application-item'}><SparkStatusBar status={'INFO'}
                                                                                                 statusText={`${application.numCores} CORES`} /></span>
                            {application.numRunning ?
                                <span className={'spark-dashboard-application-item'}><SparkStatusBar status={'RUNNING'}
                                                                                                     statusText={`${application.numRunning} RUNNING`} /></span> : null}
                            {application.numCompleted ?
                                <span className={'spark-dashboard-application-item'}><SparkStatusBar
                                    status={'COMPLETED'}
                                    statusText={`${application.numCompleted} COMPLETED`} /></span> : null}
                        </div>
                        <div>
                            <SparkJobTable items={application.jobs} />
                        </div>
                    </div> : null
                )
            }
        }</UseSignal>
    }
}
