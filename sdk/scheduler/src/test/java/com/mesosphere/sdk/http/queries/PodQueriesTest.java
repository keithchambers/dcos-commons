package com.mesosphere.sdk.http.queries;

import com.mesosphere.sdk.http.types.TaskInfoAndStatus;
import com.mesosphere.sdk.offer.CommonIdUtils;
import com.mesosphere.sdk.offer.taskdata.TaskLabelWriter;
import com.mesosphere.sdk.scheduler.Driver;
import com.mesosphere.sdk.scheduler.TaskKiller;
import com.mesosphere.sdk.scheduler.recovery.RecoveryType;
import com.mesosphere.sdk.specification.ServiceSpec;
import com.mesosphere.sdk.state.ConfigStore;
import com.mesosphere.sdk.state.GoalStateOverride;
import com.mesosphere.sdk.state.StateStore;
import com.mesosphere.sdk.testutils.TaskTestUtils;
import com.mesosphere.sdk.testutils.TestConstants;
import org.apache.mesos.Protos;
import org.apache.mesos.Protos.TaskInfo;
import org.apache.mesos.Protos.TaskState;
import org.apache.mesos.Protos.TaskStatus;
import org.apache.mesos.SchedulerDriver;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import javax.ws.rs.core.Response;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

public class PodQueriesTest {

    // build from hand to avoid default type/index settings:
    private static final TaskInfo NO_POD_TASK = Protos.TaskInfo.newBuilder()
            .setTaskId(TestConstants.TASK_ID)
            .setName(TestConstants.TASK_NAME)
            .setSlaveId(TestConstants.AGENT_ID)
            .build();
    private static final TaskStatus NO_POD_STATUS =
            TaskTestUtils.generateStatus(NO_POD_TASK.getTaskId(), TaskState.TASK_RUNNING);
    // test-0: two RUNNING tasks, one STAGING task, and one task without status:
    private static final TaskInfo POD_0_TASK_A;
    private static final TaskInfo POD_0_TASK_B;
    private static final TaskInfo POD_0_TASK_C;
    private static final TaskInfo POD_0_TASK_D;
    private static final TaskStatus POD_0_STATUS_A;
    private static final TaskStatus POD_0_STATUS_B;
    private static final TaskStatus POD_0_STATUS_C;
    // test-1: one RUNNING task, one FINISHED task:
    private static final TaskInfo POD_1_TASK_A;
    private static final TaskInfo POD_1_TASK_B;
    private static final TaskStatus POD_1_STATUS_A;
    private static final TaskStatus POD_1_STATUS_B;
    // test-2: one FAILED task:
    private static final TaskInfo POD_2_TASK_A;
    private static final TaskStatus POD_2_STATUS_A;
    static {
        // pod 0
        TaskInfo.Builder infoBuilder = NO_POD_TASK.toBuilder();
        infoBuilder.setLabels(new TaskLabelWriter(infoBuilder)
                .setType("test")
                .setIndex(0)
                .toProto());
        POD_0_TASK_A = infoBuilder.setName("test-0-a").setTaskId(CommonIdUtils.toTaskId(TestConstants.SERVICE_NAME, "a")).build();
        POD_0_STATUS_A = TaskTestUtils.generateStatus(POD_0_TASK_A.getTaskId(), TaskState.TASK_RUNNING);

        POD_0_TASK_B = POD_0_TASK_A.toBuilder().setName("test-0-b").setTaskId(CommonIdUtils.toTaskId(TestConstants.SERVICE_NAME, "b")).build();
        POD_0_STATUS_B = TaskTestUtils.generateStatus(POD_0_TASK_B.getTaskId(), TaskState.TASK_STAGING);

        POD_0_TASK_C = POD_0_TASK_A.toBuilder().setName("test-0-c").setTaskId(CommonIdUtils.toTaskId(TestConstants.SERVICE_NAME, "c")).build();
        POD_0_STATUS_C = TaskTestUtils.generateStatus(POD_0_TASK_C.getTaskId(), TaskState.TASK_RUNNING);

        POD_0_TASK_D = POD_0_TASK_A.toBuilder().setName("test-0-d").setTaskId(CommonIdUtils.toTaskId(TestConstants.SERVICE_NAME, "d")).build();

        // pod 1
        infoBuilder = POD_0_TASK_A.toBuilder();
        infoBuilder.setLabels(new TaskLabelWriter(infoBuilder).setIndex(1).toProto());
        POD_1_TASK_A = infoBuilder.setName("test-1-a").setTaskId(CommonIdUtils.toTaskId(TestConstants.SERVICE_NAME, "a")).build();
        POD_1_STATUS_A = TaskTestUtils.generateStatus(POD_1_TASK_A.getTaskId(), TaskState.TASK_FINISHED);

        POD_1_TASK_B = POD_1_TASK_A.toBuilder().setName("test-1-b").setTaskId(CommonIdUtils.toTaskId(TestConstants.SERVICE_NAME, "b")).build();
        POD_1_STATUS_B = TaskTestUtils.generateStatus(POD_1_TASK_B.getTaskId(), TaskState.TASK_RUNNING);

        // pod 2
        infoBuilder = POD_0_TASK_A.toBuilder();
        infoBuilder.setLabels(new TaskLabelWriter(infoBuilder).setIndex(2).toProto());
        POD_2_TASK_A = infoBuilder.setName("test-2-a").setTaskId(CommonIdUtils.toTaskId(TestConstants.SERVICE_NAME, "a")).build();
        POD_2_STATUS_A = TaskTestUtils.generateStatus(POD_2_TASK_A.getTaskId(), TaskState.TASK_FINISHED);
    }
    private static final Collection<TaskInfo> TASK_INFOS = Arrays.asList(
            NO_POD_TASK,
            POD_0_TASK_A,
            POD_0_TASK_B,
            POD_0_TASK_C,
            POD_0_TASK_D,
            POD_1_TASK_A,
            POD_1_TASK_B,
            POD_2_TASK_A);
    private static final Collection<TaskStatus> TASK_STATUSES = Arrays.asList(
            NO_POD_STATUS,
            POD_0_STATUS_A,
            POD_0_STATUS_B,
            POD_0_STATUS_C,
            //POD_A_STATUS_3, (none created)
            POD_1_STATUS_A,
            POD_1_STATUS_B,
            POD_2_STATUS_A);

    @Mock private StateStore mockStateStore;
    @Mock private ConfigStore<ServiceSpec> mockConfigStore;
    @Mock private SchedulerDriver mockDriver;
    @Mock private PodQueries.FailureSetter mockFailureSetter;

    @BeforeClass
    public static void beforeAll() throws InterruptedException {
        TaskKiller.reset(false); // disable background executor to avoid unexpected calls
    }

    @AfterClass
    public static void afterAll() throws InterruptedException {
        TaskKiller.reset(true); // reenable background executor to return to default behavior
    }

    @Before
    public void beforeEach() {
        MockitoAnnotations.initMocks(this);
        Driver.setDriver(mockDriver);
    }

    @Test
    public void testGetPodNames() {
        when(mockStateStore.fetchTasks()).thenReturn(TASK_INFOS);
        Response response = PodQueries.list(mockStateStore);
        assertEquals(200, response.getStatus());
        JSONArray json = new JSONArray((String) response.getEntity());
        assertEquals(json.toString(), 4, json.length());

        assertEquals("test-0", json.get(0));
        assertEquals("test-1", json.get(1));
        assertEquals("test-2", json.get(2));
        assertEquals("UNKNOWN_POD_test-task-name", json.get(3));
    }

    @Test
    public void testGetAllPodStatuses() throws Exception {
        when(mockStateStore.fetchTasks()).thenReturn(TASK_INFOS);
        when(mockStateStore.fetchStatuses()).thenReturn(TASK_STATUSES);
        when(mockStateStore.fetchGoalOverrideStatus(TestConstants.TASK_NAME)).thenReturn(GoalStateOverride.Status.INACTIVE);
        when(mockStateStore.fetchGoalOverrideStatus("test-0-a")).thenReturn(GoalStateOverride.Status.INACTIVE);
        when(mockStateStore.fetchGoalOverrideStatus("test-0-b"))
                .thenReturn(GoalStateOverride.NONE.newStatus(GoalStateOverride.Progress.IN_PROGRESS));
        when(mockStateStore.fetchGoalOverrideStatus("test-0-c"))
                .thenReturn(GoalStateOverride.PAUSED.newStatus(GoalStateOverride.Progress.PENDING));
        when(mockStateStore.fetchGoalOverrideStatus("test-0-d")).thenReturn(GoalStateOverride.Status.INACTIVE);
        // test-0-d lacks TaskStatus, so no override fetch
        when(mockStateStore.fetchGoalOverrideStatus("test-1-a")).thenReturn(GoalStateOverride.Status.INACTIVE);
        when(mockStateStore.fetchGoalOverrideStatus("test-1-b"))
                .thenReturn(GoalStateOverride.NONE.newStatus(GoalStateOverride.Progress.IN_PROGRESS));
        when(mockStateStore.fetchGoalOverrideStatus("test-2-a")).thenReturn(GoalStateOverride.Status.INACTIVE);
        when(mockStateStore.fetchGoalOverrideStatus(TestConstants.TASK_NAME))
                .thenReturn(GoalStateOverride.PAUSED.newStatus(GoalStateOverride.Progress.COMPLETE));
        Response response = PodQueries.getStatuses(mockStateStore, TestConstants.SERVICE_NAME);

        assertEquals(200, response.getStatus());
        JSONObject json = new JSONObject((String) response.getEntity());
        assertEquals(json.toString(), 2, json.length());

        assertEquals(TestConstants.SERVICE_NAME, json.get("service"));

        JSONArray pods = json.getJSONArray("pods");
        assertEquals(pods.toString(), 2, pods.length());

        JSONObject pod = pods.getJSONObject(0);
        assertEquals(2, pod.length());
        assertEquals("test", pod.getString("name"));
        JSONArray instances = pod.getJSONArray("instances");

        JSONObject podInstance = instances.getJSONObject(0);
        assertEquals(2, podInstance.length());
        assertEquals("test-0", podInstance.getString("name"));
        JSONArray tasks = podInstance.getJSONArray("tasks");

        JSONObject task = tasks.getJSONObject(0);
        assertEquals(3, task.length());
        assertEquals("test-0-a", task.getString("name"));
        Protos.TaskID taskId = Protos.TaskID.newBuilder().setValue(task.getString("id")).build();
        assertEquals(TestConstants.SERVICE_NAME, CommonIdUtils.toSanitizedServiceName(taskId).get());
        assertEquals("a", CommonIdUtils.toTaskName(taskId));
        assertEquals("RUNNING", task.getString("status"));

        task = tasks.getJSONObject(1);
        assertEquals(3, task.length());
        assertEquals("test-0-b", task.getString("name"));
        taskId = Protos.TaskID.newBuilder().setValue(task.getString("id")).build();
        assertEquals(TestConstants.SERVICE_NAME, CommonIdUtils.toSanitizedServiceName(taskId).get());
        assertEquals("b", CommonIdUtils.toTaskName(taskId));
        assertEquals("STARTING", task.getString("status"));

        task = tasks.getJSONObject(2);
        assertEquals(3, task.length());
        assertEquals("test-0-c", task.getString("name"));
        taskId = Protos.TaskID.newBuilder().setValue(task.getString("id")).build();
        assertEquals(TestConstants.SERVICE_NAME, CommonIdUtils.toSanitizedServiceName(taskId).get());
        assertEquals("c", CommonIdUtils.toTaskName(taskId));
        assertEquals("PAUSING", task.getString("status"));

        task = tasks.getJSONObject(3);
        assertEquals(2, task.length());
        assertEquals("test-0-d", task.getString("name"));
        taskId = Protos.TaskID.newBuilder().setValue(task.getString("id")).build();
        assertEquals(TestConstants.SERVICE_NAME, CommonIdUtils.toSanitizedServiceName(taskId).get());
        assertEquals("d", CommonIdUtils.toTaskName(taskId));

        podInstance = instances.getJSONObject(1);
        assertEquals(2, podInstance.length());
        assertEquals("test-1", podInstance.getString("name"));
        tasks = podInstance.getJSONArray("tasks");

        task = tasks.getJSONObject(0);
        assertEquals(3, task.length());
        assertEquals("test-1-a", task.getString("name"));
        taskId = Protos.TaskID.newBuilder().setValue(task.getString("id")).build();
        assertEquals(TestConstants.SERVICE_NAME, CommonIdUtils.toSanitizedServiceName(taskId).get());
        assertEquals("a", CommonIdUtils.toTaskName(taskId));
        assertEquals("FINISHED", task.getString("status"));

        task = tasks.getJSONObject(1);
        assertEquals(3, task.length());
        assertEquals("test-1-b", task.getString("name"));
        taskId = Protos.TaskID.newBuilder().setValue(task.getString("id")).build();
        assertEquals(TestConstants.SERVICE_NAME, CommonIdUtils.toSanitizedServiceName(taskId).get());
        assertEquals("b", CommonIdUtils.toTaskName(taskId));
        assertEquals("STARTING", task.getString("status"));

        podInstance = instances.getJSONObject(2);
        assertEquals(2, podInstance.length());
        assertEquals("test-2", podInstance.getString("name"));
        tasks = podInstance.getJSONArray("tasks");

        task = tasks.getJSONObject(0);
        assertEquals(3, task.length());
        assertEquals("test-2-a", task.getString("name"));
        taskId = Protos.TaskID.newBuilder().setValue(task.getString("id")).build();
        assertEquals(TestConstants.SERVICE_NAME, CommonIdUtils.toSanitizedServiceName(taskId).get());
        assertEquals("a", CommonIdUtils.toTaskName(taskId));
        assertEquals("FINISHED", task.getString("status"));

        pod = pods.getJSONObject(1);
        assertEquals(2, pod.length());
        assertEquals("UNKNOWN_POD", pod.getString("name"));
        instances = pod.getJSONArray("instances");

        podInstance = instances.getJSONObject(0);
        assertEquals(2, podInstance.length());
        assertEquals("UNKNOWN_POD-0", podInstance.getString("name"));
        tasks = podInstance.getJSONArray("tasks");

        task = tasks.getJSONObject(0);
        assertEquals(3, task.length());
        assertEquals("test-task-name", task.getString("name"));
        taskId = Protos.TaskID.newBuilder().setValue(task.getString("id")).build();
        assertEquals(TestConstants.SERVICE_NAME, CommonIdUtils.toSanitizedServiceName(taskId).get());
        assertEquals("test-task-name", CommonIdUtils.toTaskName(taskId));
        assertEquals("PAUSED", task.getString("status"));
    }

    @Test
    public void testGetPodStatus() throws Exception {
        when(mockStateStore.fetchTasks()).thenReturn(TASK_INFOS);
        when(mockStateStore.fetchStatuses()).thenReturn(TASK_STATUSES);
        when(mockStateStore.fetchGoalOverrideStatus("test-1-a")).thenReturn(GoalStateOverride.Status.INACTIVE);
        when(mockStateStore.fetchGoalOverrideStatus("test-1-b"))
                .thenReturn(GoalStateOverride.PAUSED.newStatus(GoalStateOverride.Progress.IN_PROGRESS));
        Response response = PodQueries.getStatus(mockStateStore, "test-1");
        assertEquals(200, response.getStatus());
        JSONObject json = new JSONObject((String) response.getEntity());
        assertEquals(json.toString(), 2, json.length());

        assertEquals("test-1", json.getString("name"));

        JSONArray tasks = json.getJSONArray("tasks");
        assertEquals(2, tasks.length());

        JSONObject task = tasks.getJSONObject(0);
        assertEquals(3, task.length());
        assertEquals("test-1-a", task.getString("name"));
        Protos.TaskID taskId = Protos.TaskID.newBuilder().setValue(task.getString("id")).build();
        assertEquals(TestConstants.SERVICE_NAME, CommonIdUtils.toSanitizedServiceName(taskId).get());
        assertEquals("a", CommonIdUtils.toTaskName(taskId));
        assertEquals("FINISHED", task.getString("status"));

        task = tasks.getJSONObject(1);
        assertEquals(3, task.length());
        assertEquals("test-1-b", task.getString("name"));
        taskId = Protos.TaskID.newBuilder().setValue(task.getString("id")).build();
        assertEquals(TestConstants.SERVICE_NAME, CommonIdUtils.toSanitizedServiceName(taskId).get());
        assertEquals("b", CommonIdUtils.toTaskName(taskId));
        assertEquals("PAUSING", task.getString("status"));
    }

    @Test
    public void testGetPodStatusNotFound() {
        when(mockStateStore.fetchTasks()).thenReturn(TASK_INFOS);
        when(mockStateStore.fetchStatuses()).thenReturn(TASK_STATUSES);
        Response response = PodQueries.getStatus(mockStateStore, "aaa");
        assertEquals(404, response.getStatus());
    }

    @Test
    public void testGetPodInfo() {
        when(mockStateStore.fetchTasks()).thenReturn(TASK_INFOS);
        when(mockStateStore.fetchStatuses()).thenReturn(TASK_STATUSES);
        Response response = PodQueries.getInfo(mockStateStore, "test-1");
        assertEquals(200, response.getStatus());
        @SuppressWarnings("unchecked")
        List<TaskInfoAndStatus> info = (List<TaskInfoAndStatus>) response.getEntity();
        assertEquals(2, info.size());
        assertEquals(POD_1_TASK_A, info.get(0).getInfo());
        assertEquals(Optional.of(POD_1_STATUS_A), info.get(0).getStatus());
        assertEquals(POD_1_TASK_B, info.get(1).getInfo());
        assertEquals(Optional.of(POD_1_STATUS_B), info.get(1).getStatus());
    }

    @Test
    public void testGetPodInfoNotFound() {
        when(mockStateStore.fetchTasks()).thenReturn(TASK_INFOS);
        when(mockStateStore.fetchStatuses()).thenReturn(TASK_STATUSES);
        Response response = PodQueries.getInfo(mockStateStore, "aaa");
        assertEquals(404, response.getStatus());
    }

    // pause

    @Test
    public void testPauseEntirePod() {
        when(mockStateStore.fetchTasks()).thenReturn(TASK_INFOS);
        when(mockStateStore.fetchStatuses()).thenReturn(TASK_STATUSES);
        Response response = PodQueries.pause(mockStateStore, "test-0", null);
        assertEquals(200, response.getStatus());

        JSONObject json = new JSONObject((String) response.getEntity());
        assertEquals(2, json.length());
        assertEquals("test-0", json.getString("pod"));
        assertEquals(4, json.getJSONArray("tasks").length());
        assertEquals("test-0-a", json.getJSONArray("tasks").get(0));
        assertEquals("test-0-b", json.getJSONArray("tasks").get(1));
        assertEquals("test-0-c", json.getJSONArray("tasks").get(2));
        assertEquals("test-0-d", json.getJSONArray("tasks").get(3));

        GoalStateOverride.Status expectedStatus =
                GoalStateOverride.PAUSED.newStatus(GoalStateOverride.Progress.PENDING);
        verify(mockStateStore).storeGoalOverrideStatus("test-0-a", expectedStatus);
        verify(mockStateStore).storeGoalOverrideStatus("test-0-b", expectedStatus);
        verify(mockStateStore).storeGoalOverrideStatus("test-0-c", expectedStatus);
        verify(mockStateStore).storeGoalOverrideStatus("test-0-d", expectedStatus);
    }

    @Test
    public void testPauseEntirePodNotFound() {
        when(mockStateStore.fetchTasks()).thenReturn(TASK_INFOS);
        when(mockStateStore.fetchStatuses()).thenReturn(TASK_STATUSES);
        Response response = PodQueries.pause(mockStateStore, "aaa", null);
        assertEquals(404, response.getStatus());

        verify(mockStateStore, times(0)).storeGoalOverrideStatus(any(), any());
    }

    @Test
    public void testPausePodTasks() {
        when(mockStateStore.fetchTasks()).thenReturn(TASK_INFOS);
        when(mockStateStore.fetchStatuses()).thenReturn(TASK_STATUSES);
        JSONArray jsonReq = new JSONArray();
        // allow both task names with and without a "pod-#-" prefix:
        jsonReq.put("a");
        jsonReq.put("test-0-c");
        Response response = PodQueries.pause(mockStateStore, "test-0", jsonReq.toString());
        assertEquals(200, response.getStatus());

        JSONObject json = new JSONObject((String) response.getEntity());
        assertEquals(2, json.length());
        assertEquals("test-0", json.getString("pod"));
        assertEquals(2, json.getJSONArray("tasks").length());
        assertEquals("test-0-a", json.getJSONArray("tasks").get(0));
        assertEquals("test-0-c", json.getJSONArray("tasks").get(1));

        GoalStateOverride.Status expectedStatus =
                GoalStateOverride.PAUSED.newStatus(GoalStateOverride.Progress.PENDING);
        verify(mockStateStore).storeGoalOverrideStatus("test-0-a", expectedStatus);
        verify(mockStateStore, times(0)).storeGoalOverrideStatus("test-0-b", expectedStatus);
        verify(mockStateStore).storeGoalOverrideStatus("test-0-c", expectedStatus);
        verify(mockStateStore, times(0)).storeGoalOverrideStatus("test-0-d", expectedStatus);
    }

    @Test
    public void testPausePodTasksNotFound() {
        when(mockStateStore.fetchTasks()).thenReturn(TASK_INFOS);
        when(mockStateStore.fetchStatuses()).thenReturn(TASK_STATUSES);
        JSONArray jsonReq = new JSONArray();
        jsonReq.put("a");
        jsonReq.put("test-0-c");
        jsonReq.put("e");
        Response response = PodQueries.pause(mockStateStore, "test-0", jsonReq.toString());
        assertEquals(404, response.getStatus());

        verify(mockStateStore, times(0)).storeGoalOverrideStatus(any(), any());
    }

    // restart

    @Test
    public void testRestartPodNotFound() {
        when(mockStateStore.fetchTasks()).thenReturn(TASK_INFOS);
        when(mockStateStore.fetchStatuses()).thenReturn(TASK_STATUSES);
        Response response = PodQueries.restartPod(mockStateStore, mockConfigStore, "aaa", RecoveryType.TRANSIENT, null);
        assertEquals(404, response.getStatus());
    }

    @Test
    public void testRestartPodManyRunning() {
        when(mockStateStore.fetchTasks()).thenReturn(TASK_INFOS);
        when(mockStateStore.fetchStatuses()).thenReturn(TASK_STATUSES);

        Response response = PodQueries.restartPod(mockStateStore, mockConfigStore, "test-0", RecoveryType.TRANSIENT, null);
        assertEquals(200, response.getStatus());
        JSONObject json = new JSONObject((String) response.getEntity());
        assertEquals(2, json.length());
        assertEquals("test-0", json.getString("pod"));
        assertEquals(4, json.getJSONArray("tasks").length());
        assertEquals("test-0-a", json.getJSONArray("tasks").get(0));
        assertEquals("test-0-b", json.getJSONArray("tasks").get(1));
        assertEquals("test-0-c", json.getJSONArray("tasks").get(2));
        assertEquals("test-0-d", json.getJSONArray("tasks").get(3));

        verify(mockDriver).killTask(POD_0_TASK_A.getTaskId());
        verify(mockDriver).killTask(POD_0_TASK_B.getTaskId());
        verify(mockDriver).killTask(POD_0_TASK_C.getTaskId());
        verify(mockDriver).killTask(POD_0_TASK_D.getTaskId());
        verifyNoMoreInteractions(mockDriver);

        verifyNoMoreInteractions(mockConfigStore);
    }

    @Test
    public void testRestartPodOneRunning() {
        when(mockStateStore.fetchTasks()).thenReturn(TASK_INFOS);
        when(mockStateStore.fetchStatuses()).thenReturn(TASK_STATUSES);

        Response response = PodQueries.restartPod(mockStateStore, mockConfigStore, "test-1", RecoveryType.TRANSIENT, null);
        assertEquals(200, response.getStatus());
        JSONObject json = new JSONObject((String) response.getEntity());
        assertEquals(2, json.length());
        assertEquals("test-1", json.getString("pod"));
        assertEquals(2, json.getJSONArray("tasks").length());
        assertEquals("test-1-a", json.getJSONArray("tasks").get(0));
        assertEquals("test-1-b", json.getJSONArray("tasks").get(1));

        verify(mockDriver).killTask(POD_1_TASK_A.getTaskId());
        verify(mockDriver).killTask(POD_1_TASK_B.getTaskId());
        verifyNoMoreInteractions(mockDriver);

        verifyNoMoreInteractions(mockConfigStore);
    }

    // replace

    @Test
    public void testReplacePodNotFound() {
        when(mockStateStore.fetchTasks()).thenReturn(TASK_INFOS);
        when(mockStateStore.fetchStatuses()).thenReturn(TASK_STATUSES);
        Response response = PodQueries.restartPod(mockStateStore, mockConfigStore, "aaa", RecoveryType.PERMANENT, null);
        assertEquals(404, response.getStatus());

        verifyNoMoreInteractions(mockConfigStore);
    }

    @Test
    public void testReplacePodManyRunning() {
        when(mockStateStore.fetchTasks()).thenReturn(TASK_INFOS);
        when(mockStateStore.fetchStatuses()).thenReturn(TASK_STATUSES);

        String podInstanceName = "test-0";

        Response response = PodQueries.restartPod(
                mockStateStore, mockConfigStore, podInstanceName, RecoveryType.PERMANENT, mockFailureSetter);
        assertEquals(200, response.getStatus());
        JSONObject json = new JSONObject((String) response.getEntity());
        assertEquals(2, json.length());
        assertEquals(podInstanceName, json.getString("pod"));
        assertEquals(4, json.getJSONArray("tasks").length());
        assertEquals(podInstanceName + "-a", json.getJSONArray("tasks").get(0));
        assertEquals(podInstanceName + "-b", json.getJSONArray("tasks").get(1));
        assertEquals(podInstanceName + "-c", json.getJSONArray("tasks").get(2));
        assertEquals(podInstanceName + "-d", json.getJSONArray("tasks").get(3));

        verify(mockDriver).killTask(POD_0_TASK_A.getTaskId());
        verify(mockDriver).killTask(POD_0_TASK_B.getTaskId());
        verify(mockDriver).killTask(POD_0_TASK_C.getTaskId());
        verify(mockDriver).killTask(POD_0_TASK_D.getTaskId());
        verifyNoMoreInteractions(mockDriver);

        List<Protos.TaskInfo> expectedFailedTasks = TASK_INFOS.stream()
                .filter(taskInfo -> taskInfo.getName().startsWith(podInstanceName))
                .collect(Collectors.toList());
        verify(mockFailureSetter, times(1)).setFailure(mockConfigStore, mockStateStore, expectedFailedTasks);
        verifyNoMoreInteractions(mockFailureSetter);
    }

    @Test
    public void testReplacePodOneRunning() {
        when(mockStateStore.fetchTasks()).thenReturn(TASK_INFOS);
        when(mockStateStore.fetchStatuses()).thenReturn(TASK_STATUSES);

        String podInstanceName = "test-1";

        Response response = PodQueries.restartPod(
                mockStateStore, mockConfigStore, podInstanceName, RecoveryType.PERMANENT, mockFailureSetter);
        assertEquals(200, response.getStatus());
        JSONObject json = new JSONObject((String) response.getEntity());
        assertEquals(2, json.length());
        assertEquals(podInstanceName, json.getString("pod"));
        assertEquals(2, json.getJSONArray("tasks").length());
        assertEquals(podInstanceName + "-a", json.getJSONArray("tasks").get(0));
        assertEquals(podInstanceName + "-b", json.getJSONArray("tasks").get(1));

        verify(mockDriver).killTask(POD_1_TASK_A.getTaskId());
        verify(mockDriver).killTask(POD_1_TASK_B.getTaskId());
        verify(mockDriver, times(2)).killTask(any());

        List<Protos.TaskInfo> expectedFailedTasks = TASK_INFOS.stream()
                .filter(taskInfo -> taskInfo.getName().startsWith(podInstanceName))
                .collect(Collectors.toList());
        verify(mockFailureSetter, times(1)).setFailure(mockConfigStore, mockStateStore, expectedFailedTasks);
        verifyNoMoreInteractions(mockFailureSetter);
    }
}
