import time
from models import Task, TaskPriority, TaskStatus, WorkerInfo, WorkerLanguage, WorkerStatus
from server.queue_manager import QueueManager

def test_priority_scheduling_with_aging():
    qm = QueueManager(scheduling_algorithm="PRIORITY_WITH_AGING")

    # Enqueue Normal priority task first
    t1 = Task(task_type="prime_sieve", priority=TaskPriority.NORMAL)
    qm.enqueue(t1)

    # Enqueue High priority task second
    t2 = Task(task_type="prime_sieve", priority=TaskPriority.HIGH)
    qm.enqueue(t2)

    # Worker requests task
    worker = WorkerInfo(
        worker_id="test-worker-1",
        language=WorkerLanguage.PYTHON,
        hostname="localhost",
        status=WorkerStatus.ONLINE
    )

    # High priority t2 should be dequeued first even though t1 was enqueued earlier
    dequeued = qm.dequeue_for_worker(worker)
    assert dequeued is not None
    assert dequeued.task_id == t2.task_id, f"Expected HIGH priority task {t2.task_id}, got {dequeued.task_id}"

    # Next should be t1
    dequeued_next = qm.dequeue_for_worker(worker)
    assert dequeued_next is not None
    assert dequeued_next.task_id == t1.task_id

    print("[OK] Priority scheduling test passed successfully!")

def test_language_runtime_constraint():
    qm = QueueManager()

    # Task requiring Java
    t_java = Task(task_type="matrix_multiply", required_language=WorkerLanguage.JAVA)
    qm.enqueue(t_java)

    # Task requiring Python
    t_py = Task(task_type="image_process", required_language=WorkerLanguage.PYTHON)
    qm.enqueue(t_py)

    python_worker = WorkerInfo(worker_id="py-1", language=WorkerLanguage.PYTHON, hostname="localhost")
    java_worker = WorkerInfo(worker_id="java-1", language=WorkerLanguage.JAVA, hostname="localhost")

    # Python worker should only get python task
    picked_py = qm.dequeue_for_worker(python_worker)
    assert picked_py.task_id == t_py.task_id, "Python worker received non-matching task!"

    # Java worker should only get java task
    picked_java = qm.dequeue_for_worker(java_worker)
    assert picked_java.task_id == t_java.task_id, "Java worker received non-matching task!"

    print("[OK] Language runtime constraint test passed successfully!")

if __name__ == "__main__":
    test_priority_scheduling_with_aging()
    test_language_runtime_constraint()
    print("\n[OK] All Queue tests passed!")
