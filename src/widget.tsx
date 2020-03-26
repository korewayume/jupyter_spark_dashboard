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
import {LabIcon} from "@jupyterlab/ui-components";
import TimeAgo from 'timeago-react';
import {ElementAttrs, VirtualElement, VirtualNode} from "@lumino/virtualdom";

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

interface ISubmissionTime {
    submissionTime: number;
}

interface IDuration extends ISubmissionTime {
    completionTime: number;
}

interface SparkJobItemState {
    job: SparkJob,
    isCollapse: boolean
}

export class SparkIcon implements VirtualElement.IRenderer {
    render(host: HTMLElement, options?: {
        attrs?: ElementAttrs;
        children?: ReadonlyArray<VirtualNode>;
    }) {
        this.unrender(host, options);
        const svg = document.createElementNS("http://www.w3.org/2000/svg", 'svg');
        svg.setAttribute('id', 'spark-icon');
        svg.setAttribute("version", "1.1");
        svg.setAttribute('viewBox', "120 0 1353 828");
        svg.innerHTML = `<path d="M308.775702 528.152668l47.30547-38.274426s-22.362586-45.155221-77.839-45.155221c-31.82368 0-59.346862 14.191641-80.419299 39.134525s-18.922188 58.916812-18.922188 64.937508c0 0 5.590646 19.782287 13.761592 30.963581s60.206961 65.367558 70.528154 76.118801c0 0 11.611343 30.96358-2.580298 42.144873s-27.093133 14.621691-39.994624 14.62169c0 0-34.834028-9.891144-34.834028-42.574922l-56.336514 30.53353s8.600994 63.21731 81.279398 63.21731 87.300094-35.694127 87.300094-35.694127 27.523182-27.523182 27.523182-66.657708c0 0 8.170945-32.683779-39.564574-79.129149 0 0-47.735519-36.984276-47.73552-62.78726 0 0 5.160597-27.523182 32.25373-22.362586s38.274425 30.96358 38.274425 30.963581zM479.935493 525.715146s-100.918909-6.880796-127.867545 96.905685C333.718586 694.869184 323.397393 827.037226 323.397393 827.037226h49.885768l9.461094-85.149845s72.535627 52.178793 155.734767-12.423277c37.787609-40.902889 39.110442-62.993684 45.241231-85.055234 5.735143-40.137401-12.614219-115.826152-103.78476-118.693724zM459.293106 710.923801c-32.94869 0-62.549872-26.89359-62.549873-62.897353 0-36.007203 41.642575-71.278162 74.591265-71.278161s61.354334 27.640156 61.354334 63.647359c0 36.005483-40.447037 70.528155-73.395726 70.528155zM827.41567 755.648972h-47.30547l11.181293-95.471039s6.020696-85.149845-58.486762-85.149845-75.688752 68.807956-75.688752 68.807956-2.867572 57.33939 44.152345 67.660583 70.528155-24.655611 70.528155-24.655611l-10.321193 61.354334s-64.221906 40.135681-129.014918-17.776535c0 0-26.950356-24.081064-26.950356-78.554603 0 0-2.867572-50.460314 48.165569-93.465287s97.478511-32.77839 97.478511-32.77839 62.499987-0.478215 92.316194 78.650934c2.294745 22.362586 0 39.566295 0 45.872544S827.41567 755.648972 827.41567 755.648972z" fill="#414042" p-id="2296"></path><path d="M982.806397 532.310388l-6.020696 48.16557h-32.969332s-13.763311 2.865851-17.776536 16.054616L903.964521 755.076146H854.938852l18.634915-157.685473s-1.147373-22.937132 22.362586-46.44537c0 0 16.629163-18.062088 34.691251-18.634915h52.178793zM1016.207499 492.88859l-35.264077 262.187556h55.046364l17.201989-120.271147L1137.051472 755.076146h64.077409l-104.072034-144.353931 63.647359-69.238006-9.461094-54.616315-91.170541 100.201586 15.05174-135.895713z" fill="#414042" p-id="2297"></path><path d="M1168.015052 489.018143l82.139497 22.981857-49.025668-96.52036 74.398602-91.170542-111.382879 26.663083-54.616315-90.310442-21.932536 112.673028-110.952829 37.844376 81.709448 35.264077L1002.875958 483.857546l-65.797608-27.953232s-36.124177-16.34189-36.124177-44.725171 36.984276-38.274425 36.984276-38.274426L1047.601129 338.788012l18.349362-106.652331s8.600994-42.432146 37.844375-42.432147 41.284774 33.831152 56.193738 56.193738L1190.377637 296.930413l108.947077-28.670555s89.450343-18.922188 36.124177 57.339389l-77.40895 90.597716 45.870824 89.450342s35.54963 72.8229-48.165569 63.074533l-79.12915-25.230157-8.600994-54.473538z" fill="#E16A1A"></path>`;
        host.appendChild(svg);
        host.style.height = '23px';
        host.style.width = '32px';
    }

    unrender(host: HTMLElement, options?: {
        attrs?: ElementAttrs;
        children?: ReadonlyArray<VirtualNode>;
    }) {
        const svg = host.querySelector('#spark-icon');
        svg && svg.remove();
    }
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

const SparkStatusBar: React.FunctionComponent<ISparkStatusBarData> = (props) => {
    const getClassName = (status: string) => [status, 'spark-status-bar'].join(' ');
    return <div className={getClassName(props.status)}>{props.statusText}</div>;
};

const SubmissionTime: React.FunctionComponent<ISubmissionTime> = (props) => {
    return (
        props.submissionTime > 0 ? <TimeAgo locale='zh_CN' datetime={new Date(props.submissionTime)} /> :
            <span>未知</span>
    )
};

const Duration: React.FunctionComponent<IDuration> = (props) => {
    return (
        <span>
            {
                props.completionTime - props.submissionTime > 0 ?
                    (moment.duration(props.completionTime - props.submissionTime) as any)
                        .format("d[d] h[h]:mm[m]:ss[s]") : '未知'
            }
        </span>
    )
};

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

    get collapseButtonClassName() {
        return this.state.isCollapse ? 'collapse collapse-button is-collapse' : 'collapse collapse-button';
    }

    render() {
        return [
            <tr key={`job-${this.state.job.id}`}>
                <td className={this.collapseButtonClassName} onClick={this.toggleCollapse.bind(this)}>
                    <LabIcon.resolveReact
                        icon={"ui-components:run"}
                        className="collapse-button-icon"
                        tag="span"
                    />
                </td>
                <td>{this.state.job.id}</td>
                <td><SparkStatusBar status={this.state.job.preferredStatus} statusText={this.state.job.preferredStatus} /></td>
                <td>{this.state.job.stageSummary}</td>
                <td><TaskProgressBar data={this.state.job} /></td>
                <td><SubmissionTime submissionTime={this.state.job.submissionTime} /></td>
                <td><Duration submissionTime={this.state.job.submissionTime}
                              completionTime={this.state.job.completionTime} /></td>
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
                {lodash.map(this.state.items, job => <SparkJobItem key={`job-${job.id}`} data={job} />)}
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
                <td><SparkStatusBar status={stage.preferredStatus} statusText={stage.preferredStatus} /></td>
                <td><TaskProgressBar data={stage} /></td>
                <td><SubmissionTime submissionTime={stage.submissionTime} /></td>
                <td><Duration submissionTime={stage.submissionTime} completionTime={stage.completionTime} /></td>
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

    get preferredStatus() {
        return this.status === 'FAILED' && (this.tasks.map(task => task.status).includes('KILLED')) ? 'KILLED' : this.status;

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

    get preferredStatus() {
        return this.status === 'FAILED' && (this.stages.map(stage => stage.preferredStatus).includes('KILLED')) ? 'KILLED' : this.status;
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
            console.log("SparkDashboardNotebook onCommClose", msg.content.data);
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
                            <span className={'spark-dashboard-application-item'}>{application.appId}</span>
                            <span className={'spark-dashboard-application-item'}>{application.appName}</span>
                            <span className={'spark-dashboard-application-item'}>{application.sparkUser}</span>
                            <span className={'spark-dashboard-application-item'}>
                                <SparkStatusBar status={'INFO'} statusText={`${application.numCores} CORES`} />
                            </span>
                            {application.numRunning ?
                                <span className={'spark-dashboard-application-item'}>
                                    <SparkStatusBar status={'RUNNING'} statusText={`${application.numRunning} RUNNING`} />
                                </span> : null}
                            {application.numCompleted ?
                                <span className={'spark-dashboard-application-item'}>
                                    <SparkStatusBar status={'COMPLETED'} statusText={`${application.numCompleted} COMPLETED`} />
                                </span> : null}
                        </div>
                        <div>
                            <SparkJobTable items={application.jobs} />
                        </div>
                    </div> : <div className="spark-dashboard"><div className="spark-dashboard-placeholder">
                        正在获取数据...
                    </div></div>
                )
            }
        }</UseSignal>
    }
}
