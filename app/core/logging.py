import logging
import sys
# pyrefly: ignore [missing-import]
from loguru import logger
from app.core.config import settings


class InterceptHandler(logging.Handler):
    """
    Handler para interceptar logs do módulo de logging padrão do Python
    e redirecioná-los para o Loguru.
    """
    def emit(self, record: logging.LogRecord) -> None:
        # Obter nível correspondente no Loguru
        try:
            level = logger.level(record.levelname).name
        except ValueError:
            level = record.levelno

        # Encontrar de onde veio a mensagem logada
        frame, depth = logging.currentframe(), 2
        while frame and frame.f_code.co_filename == logging.__file__:
            frame = frame.f_back
            depth += 1

        logger.opt(depth=depth, exception=record.exc_info).log(
            level, record.getMessage()
        )


def setup_logging() -> None:
    """
    Configura o loguru como o logger primário da aplicação,
    interceptando logs padrão do Uvicorn e FastAPI.
    """
    # Determinar o nível de log com base no modo de depuração
    log_level = "DEBUG" if settings.DEBUG else "INFO"

    # Remover handlers padrão
    logger.remove()

    # Adicionar stdout handler formatado
    log_format = (
        "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | "
        "<level>{level: <8}</level> | "
        "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - "
        "<level>{message}</level>"
    )

    if settings.APP_ENV == "production":
        # Em produção, podemos usar formato estruturado JSON
        logger.add(
            sys.stdout,
            level=log_level,
            format="{message}",
            serialize=True,  # Converte para JSON automaticamente
        )
    else:
        # Em desenvolvimento, usamos formato colorido amigável
        logger.add(
            sys.stdout,
            level=log_level,
            format=log_format,
            colorize=True,
        )

    # Interceptar logs de outros frameworks (uvicorn, fastapi, etc.)
    logging.basicConfig(handlers=[InterceptHandler()], level=0, force=True)
    
    for logger_name in ("uvicorn", "uvicorn.access", "uvicorn.error", "fastapi"):
        logging_logger = logging.getLogger(logger_name)
        logging_logger.handlers = [InterceptHandler()]
        logging_logger.propagate = False

    logger.info("Logs estruturados e interceptadores inicializados com sucesso.")
BaseLogger = logger
