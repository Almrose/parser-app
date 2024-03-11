
from pathlib import Path
from datetime import timedelta

# import environ

ROOT_DIR = Path(__file__).resolve(strict=True).parent.parent.parent
# test_project/
APPS_DIR = ROOT_DIR / "parser-app"
# env = environ.Env()

# READ_DOT_ENV_FILE = env.bool("READ_DOT_ENV_FILE", default=False)
# if READ_DOT_ENV_FILE:
#     # OS environment variables take precedence over variables from .env
#     env.read_env(str(ROOT_DIR / ".env"))

# GENERAL
# ------------------------------------------------------------------------------
# Local time zone. Choices are
# http://en.wikipedia.org/wiki/List_of_tz_zones_by_name
# though not all of them may be available with every OS.
# In Windows, this must be set to your system time zone.
TIME_ZONE = "Europe/Moscow"
# https://docs.djangoproject.com/en/dev/ref/settings/#language-code
LANGUAGE_CODE = "en-us"
# https://docs.djangoproject.com/en/dev/ref/settings/#site-id
SITE_ID = 1
# https://docs.djangoproject.com/en/dev/ref/settings/#use-i18n
USE_I18N = True
# https://docs.djangoproject.com/en/dev/ref/settings/#use-l10n
USE_L10N = True
# https://docs.djangoproject.com/en/dev/ref/settings/#use-tz
USE_TZ = True
# https://docs.djangoproject.com/en/dev/ref/settings/#locale-paths
LOCALE_PATHS = [str(ROOT_DIR / "locale")]


# Celery
# ------------------------------------------------------------------------------
if USE_TZ:
    # https://docs.celeryq.dev/en/stable/userguide/configuration.html#std:setting-timezone
    TIMEZONE = TIME_ZONE
# https://docs.celeryq.dev/en/stable/userguide/configuration.html#std:setting-broker_url
CELERY_BROKER_URL = "redis://redis:6379/0"
# CELERY_BROKER_URL = 'amqp://guest@localhost//'
# https://docs.celeryq.dev/en/stable/userguide/configuration.html#std:setting-result_backend
RESULT_BACKEND = CELERY_BROKER_URL
# https://docs.celeryq.dev/en/stable/userguide/configuration.html#std:setting-accept_content
ACCEPT_CONTENT = ["json"]
# https://docs.celeryq.dev/en/stable/userguide/configuration.html#std:setting-task_serializer
TASK_SERIALIZER = "json"
# https://docs.celeryq.dev/en/stable/userguide/configuration.html#std:setting-result_serializer
RESULT_SERIALIZER = "json"
CELERY_IMPORTS = ("worker.tasks")
# https://docs.celeryq.dev/en/stable/userguide/configuration.html#task-time-limit
# TODO: set to whatever value is adequate in your circumstances
CELERY_TASK_TIME_LIMIT = 5 * 60
# https://docs.celeryq.dev/en/stable/userguide/configuration.html#task-soft-time-limit
# TODO: set to whatever value is adequate in your circumstances
CELERY_TASK_SOFT_TIME_LIMIT = 60
# https://docs.celeryq.dev/en/stable/userguide/configuration.html#beat-scheduler
# CELERY_BEAT_SCHEDULER = "django_celery_beat.schedulers:DatabaseScheduler"
# CELERY_RESULT_BACKEND = 'rpc://'

# CELERYBEAT_SCHEDULE = {
#     'resume_list_parse': {
#         'task': 'worker.tasks.parse_last_resumes',
#         'schedule': timedelta(seconds=5)  # execute every minute
#     },
#     # 'bad_consultation_check': {
#     #     'task': 'medoo_doctor.coreBot.tasks.clear_bad_covs',
#     #     'schedule': timedelta(seconds=60)  # execute every minute
#     # }
#     'print_test': {
#         'task': "worker.tasks.print_test",
#         'schedule': timedelta(seconds=5)
#     }
# }
