#!/usr/bin/env python
"""
Script para probar el agrupamiento de subtítulos por episodio.
Descarga y procesa un ZIP de subtítulos para verificar que se agrupen correctamente.
"""

import os
import sys
import logging
import time
from pathlib import Path
from corpus_scraper.orchestrator import Orchestrator
from corpus_scraper.config_manager import ConfigManager
from corpus_scraper.extractor import Extractor

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

def test_subtitle_grouping(url=None):
    """
    Prueba el agrupamiento de subtítulos por episodio.
    
    Args:
        url: URL opcional para descargar subtítulos. Si es None, se usa un ejemplo.
    """
    # URL de ejemplo (subtítulos de Los Simpson)
    if url is None:
        url = "https://www.opensubtitles.org/download/sub/8278234"
    
    # Inicializar orquestador con configuración mínima
    orchestrator = Orchestrator()
    
    # Crear un registro URL simulado para procesar
    url_record = {
        'url': url,
        'url_hash': 'test_hash',
        'source': 'opensubtitles',
        'created_at': time.time(),
        'status': 'pending'
    }
    
    logger.info(f"Procesando URL de prueba: {url}")
    
    # Procesar la URL
    result = orchestrator._process_single_url(url_record)
    
    if result.get('success'):
        logger.info(f"Procesamiento exitoso: {result}")
        
        # Mostrar información sobre los archivos guardados
        if 'file_paths' in result:
            for file_path in result['file_paths']:
                if file_path:
                    logger.info(f"Archivo guardado: {file_path}")
                    
                    # Mostrar las primeras líneas del archivo
                    try:
                        with open(file_path, 'r', encoding='utf-8') as f:
                            content = f.read(2000)  # Leer primeros 2000 caracteres
                            logger.info(f"Primeras líneas del archivo:")
                            logger.info(content[:500] + "..." if len(content) > 500 else content)
                    except Exception as e:
                        logger.error(f"Error al leer el archivo: {e}")
    else:
        logger.error(f"Error en el procesamiento: {result.get('error')}")
    
    return result

if __name__ == "__main__":
    # Permitir que se pase una URL como argumento
    url = None
    if len(sys.argv) > 1:
        url = sys.argv[1]
        
    test_subtitle_grouping(url)
