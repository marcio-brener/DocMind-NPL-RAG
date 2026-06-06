from fastapi import APIRouter, HTTPException, status

from app.core.logging import BaseLogger
from app.schemas.task import TaskResponse
from app.services.task_service import task_service

router = APIRouter()


@router.get(
    "/{task_id}",
    response_model=TaskResponse,
    status_code=status.HTTP_200_OK,
    summary="Consultar status de tarefa de processamento",
    description=(
        "Retorna o status atual, progresso percentual e informações detalhadas "
        "de uma tarefa de processamento assíncrono de documento. "
        "Use o task_id retornado pelo endpoint de upload para acompanhar o processamento."
    ),
)
async def get_task_status(task_id: str) -> TaskResponse:
    """
    Consulta o estado atual de uma tarefa de processamento assíncrono.

    Args:
        task_id: Identificador único da tarefa (UUID retornado no upload).

    Returns:
        TaskResponse com status, progresso (0–100) e mensagem descritiva.

    Raises:
        404: Se a tarefa não for encontrada.
    """
    BaseLogger.info(f"[Tasks] Consulta de status: task_id={task_id}")

    task = task_service.get_task(task_id)

    if not task:
        BaseLogger.warning(
            f"[Tasks] Tarefa não encontrada: task_id={task_id}"
        )
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=(
                f"Tarefa '{task_id}' não encontrada. "
                "Verifique se o task_id está correto."
            ),
        )

    BaseLogger.info(
        f"[Tasks] Status retornado: task_id={task_id} | "
        f"status={task.status.value} | progresso={task.progress}%"
    )

    return task
