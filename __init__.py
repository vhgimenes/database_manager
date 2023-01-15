# Importando libs externas
import importlib
# Importando as funções do pacote
from .  import connect_tenax_db
from . import queries_tenax_db
# Para permitir que mudanças passem a ser usadas diretamente
importlib.reload(connect_tenax_db)
importlib.reload(queries_tenax_db)
