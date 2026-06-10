import json
import os
import threading
from datetime import datetime
from typing import Optional

from app.core.config import settings
from app.core.logging import BaseLogger
from app.schemas.task import TaskCreate, TaskResponse, TaskStatus

# Caminho do arquivo JSON de persistência das tarefas
_TASKS_FILE_PATH = os.path.join(
    os.path.dirname(settings.UPLOAD_DIR), "tasks.json"
)


class TaskService:
    """
    Serviço corporativo de rastreamento de tarefas de processamento.

    Persiste o estado das tarefas em um arquivo JSON local (data/tasks.json),
    garantindo compartilhamento de estado entre o servidor FastAPI e os workers
    sem necessidade de dependência externa adicional (ex.: Redis).

    Thread-safe via threading.Lock para acesso concorrente seguro.

    Status possíveis: QUEUED → PROCESSING → COMPLETED | FAILED
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._ensure_tasks_file()
        BaseLogger.debug(
            f"[TaskService] Inicializado. Persistência em: {_TASKS_FILE_PATH}"
        )

    # ── Internos ─────────────────────────────────────────────────────────────

    def _ensure_tasks_file(self) -> None:
        """Garante que o diretório e arquivo JSON de tarefas existam."""
        os.makedirs(os.path.dirname(_TASKS_FILE_PATH), exist_ok=True)
        if not os.path.exists(_TASKS_FILE_PATH):
            with open(_TASKS_FILE_PATH, "w", encoding="utf-8") as f:
                json.dump({}, f)
            BaseLogger.debug(
                f"[TaskService] Arquivo de tarefas criado em: {_TASKS_FILE_PATH}"
            )

    def _load_tasks(self) -> dict:
        """Lê o arquivo JSON e retorna o dicionário de tarefas."""
        try:
            with open(_TASKS_FILE_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, FileNotFoundError):
            BaseLogger.warning(
                "[TaskService] Arquivo de tarefas corrompido ou ausente. Reiniciando."
            )
            return {}

    def _save_tasks(self, tasks: dict) -> None:
        """Persiste o dicionário de tarefas no arquivo JSON."""
        with open(_TASKS_FILE_PATH, "w", encoding="utf-8") as f:
            json.dump(tasks, f, default=str, ensure_ascii=False, indent=2)

    def _serialize_task(self, task_data: dict) -> TaskResponse:
        """Converte um dicionário bruto do JSON em um TaskResponse tipado."""
        return TaskResponse(
            task_id=task_data["task_id"],
            document_id=task_data.get("document_id"),
            filename=task_data.get("filename"),
            status=TaskStatus(task_data["status"]),
            progress=task_data.get("progress", 0),
            message=task_data.get("message", ""),
            created_at=datetime.fromisoformat(task_data["created_at"]),
            updated_at=datetime.fromisoformat(task_data["updated_at"]),
            error_detail=task_data.get("error_detail") or task_data.get("error"),
            result=task_data.get("result"),
            error=task_data.get("error") or task_data.get("error_detail"),
        )

    # ── Interface Pública ─────────────────────────────────────────────────────

    def create_task(self, task_input: TaskCreate) -> TaskResponse:
        """
        Cria uma nova tarefa com status inicial QUEUED e a persiste.

        Args:
            task_input: Dados de criação da tarefa (task_id, document_id, filename).

        Returns:
            TaskResponse com os dados completos da tarefa criada.
        """
        now = datetime.utcnow()
        task_data = {
            "task_id": task_input.task_id,
            "document_id": task_input.document_id,
            "filename": task_input.filename,
            "status": TaskStatus.QUEUED.value,
            "progress": 0,
            "message": "Tarefa enfileirada aguardando processamento.",
            "created_at": now.isoformat(),
            "updated_at": now.isoformat(),
            "error_detail": None,
            "error": None,
            "result": None,
        }

        with self._lock:
            tasks = self._load_tasks()
            tasks[task_input.task_id] = task_data
            self._save_tasks(tasks)

        BaseLogger.info(
            f"[TaskService] Tarefa criada: task_id={task_input.task_id} | "
            f"doc_id={task_input.document_id} | arquivo={task_input.filename}"
        )
        return self._serialize_task(task_data)

    def update_task(
        self,
        task_id: str,
        status: TaskStatus,
        progress: int = 0,
        message: str = "",
        error_detail: Optional[str] = None,
        result: Optional[dict] = None,
    ) -> Optional[TaskResponse]:
        """
        Atualiza o status, progresso e mensagem de uma tarefa existente.

        Args:
            task_id: Identificador único da tarefa.
            status: Novo status (QUEUED, PROCESSING, COMPLETED, FAILED).
            progress: Percentual de conclusão (0–100).
            message: Mensagem descritiva do estado atual.
            error_detail: Detalhes do erro em caso de falha.

        Returns:
            TaskResponse atualizado ou None se a tarefa não for encontrada.
        """
        with self._lock:
            tasks = self._load_tasks()
            if task_id not in tasks:
                BaseLogger.warning(
                    f"[TaskService] Tentativa de atualizar tarefa inexistente: {task_id}"
                )
                return None

            tasks[task_id]["status"] = status.value
            tasks[task_id]["progress"] = progress
            tasks[task_id]["message"] = message
            tasks[task_id]["updated_at"] = datetime.utcnow().isoformat()
            if error_detail is not None:
                tasks[task_id]["error_detail"] = error_detail
                tasks[task_id]["error"] = error_detail
            if result is not None:
                tasks[task_id]["result"] = result

            self._save_tasks(tasks)
            task_data = tasks[task_id]

        BaseLogger.info(
            f"[TaskService] Tarefa atualizada: task_id={task_id} | "
            f"status={status.value} | progresso={progress}%"
        )
        return self._serialize_task(task_data)

    def get_task(self, task_id: str) -> Optional[TaskResponse]:
        """
        Retorna os dados de uma tarefa pelo seu ID.

        Args:
            task_id: Identificador único da tarefa.

        Returns:
            TaskResponse ou None se não encontrada.
        """
        with self._lock:
            tasks = self._load_tasks()
            task_data = tasks.get(task_id)

        if not task_data:
            BaseLogger.debug(
                f"[TaskService] Tarefa não encontrada: {task_id}"
            )
            return None

        return self._serialize_task(task_data)


# Instanciação Singleton do serviço de tarefas
task_service = TaskService()
