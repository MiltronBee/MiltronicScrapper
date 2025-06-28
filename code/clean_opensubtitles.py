#!/usr/bin/env python
"""
Script para eliminar archivos .txt individuales del directorio OpenSubtitles,
manteniendo intacta la carpeta combined y su contenido.
"""

import os
import shutil
import sys
import logging
from pathlib import Path

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

def clean_opensubtitles_directory(opensubtitles_dir):
    """
    Elimina todos los archivos .txt del directorio OpenSubtitles,
    excepto los que están en la carpeta combined.
    
    Args:
        opensubtitles_dir: Ruta al directorio OpenSubtitles
    """
    opensubtitles_path = Path(opensubtitles_dir)
    combined_dir = opensubtitles_path / "combined"
    
    # Verificar que exista el directorio combined
    if not combined_dir.exists() or not combined_dir.is_dir():
        logger.error(f"¡Error! La carpeta 'combined' no existe en {opensubtitles_dir}")
        logger.error("No se eliminarán los archivos. Ejecute primero opensubtitles_formatter.py")
        return False
    
    # Contar los archivos combinados para verificar que el proceso fue exitoso
    combined_files = list(combined_dir.glob("*.txt"))
    if not combined_files:
        logger.error(f"¡Error! La carpeta 'combined' está vacía. No se eliminarán los archivos.")
        return False
    
    logger.info(f"Directorio combined contiene {len(combined_files)} archivos")
    
    # Buscar todos los archivos .txt individuales
    individual_files = []
    for file_path in opensubtitles_path.glob("*.txt"):
        if file_path.is_file():
            individual_files.append(file_path)
    
    logger.info(f"Encontrados {len(individual_files)} archivos .txt individuales para eliminar")
    
    # Confirmar con el usuario
    if len(individual_files) == 0:
        logger.info("No hay archivos individuales para eliminar")
        return True

    # Eliminar los archivos individuales
    for file_path in individual_files:
        try:
            os.remove(file_path)
            if len(individual_files) < 10 or len(individual_files) % 1000 == 0:
                logger.info(f"Eliminado: {file_path.name}")
        except Exception as e:
            logger.error(f"Error al eliminar {file_path}: {e}")
    
    logger.info(f"Eliminados {len(individual_files)} archivos .txt individuales")
    
    # Verificar si quedan archivos individuales
    remaining_files = list(opensubtitles_path.glob("*.txt"))
    if remaining_files:
        logger.warning(f"Quedan {len(remaining_files)} archivos .txt en el directorio")
    else:
        logger.info("Todos los archivos .txt individuales fueron eliminados correctamente")
    
    # Calcular espacio liberado
    return True

if __name__ == "__main__":
    opensubtitles_dir = "/mnt/c/Users/vulca/proyects/MiltronicScrapper/code/data/corpus_raw/opensubtitles"
    
    if len(sys.argv) > 1:
        opensubtitles_dir = sys.argv[1]
    
    if not os.path.exists(opensubtitles_dir):
        print(f"Error: El directorio {opensubtitles_dir} no existe")
        sys.exit(1)
    
    logger.info(f"Limpiando el directorio: {opensubtitles_dir}")
    clean_opensubtitles_directory(opensubtitles_dir)
