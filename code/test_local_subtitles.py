#!/usr/bin/env python
"""
Script para probar el agrupamiento de subtítulos con archivos locales.
Crea archivos SRT de prueba y los procesa con el método actualizado del orquestador.
"""

import os
import sys
import logging
import tempfile
import zipfile
import shutil
import time
from pathlib import Path
import io
from corpus_scraper.orchestrator import Orchestrator
from corpus_scraper.extractor import Extractor

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

def create_test_srt_files():
    """
    Crea un conjunto de archivos SRT de prueba para un episodio simulado.
    
    Returns:
        Ruta al archivo ZIP que contiene los archivos SRT
    """
    # Crear directorio temporal
    temp_dir = tempfile.mkdtemp()
    
    # Crear un conjunto de archivos SRT
    episode_names = ["simpson_episode1", "simpson_episode2"]
    entries_per_episode = 5
    
    # Para cada episodio, crear varios archivos de entrada
    for episode in episode_names:
        for entry in range(1, entries_per_episode + 1):
            filename = f"{episode}_entry{entry}.srt"
            filepath = os.path.join(temp_dir, filename)
            
            # Crear contenido SRT simple
            content = f"""1
00:00:01,000 --> 00:00:05,000
Este es el diálogo {entry} del episodio {episode}.
Segunda línea del mismo diálogo.

2
00:00:06,000 --> 00:00:10,000
Otro diálogo para el mismo episodio.

3
00:00:11,000 --> 00:00:15,000
Tercer diálogo de prueba.
"""
            # Guardar el archivo SRT
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(content)
            
            logger.info(f"Creado archivo SRT de prueba: {filename}")
    
    # Crear un archivo ZIP con todos los archivos SRT
    zip_path = os.path.join(temp_dir, "test_subtitles.zip")
    with zipfile.ZipFile(zip_path, 'w') as zipf:
        for file in os.listdir(temp_dir):
            if file.endswith('.srt'):
                zipf.write(os.path.join(temp_dir, file), file)
    
    logger.info(f"Creado archivo ZIP con todos los SRT: {zip_path}")
    return zip_path, temp_dir

def test_subtitle_grouping():
    """
    Prueba el agrupamiento de subtítulos localmente.
    """
    # Crear archivos SRT de prueba
    zip_path, temp_dir = create_test_srt_files()
    
    try:
        # Inicializar orquestador
        orchestrator = Orchestrator()
        
        # Leer el archivo ZIP
        with open(zip_path, 'rb') as f:
            content_bytes = f.read()
        
        # Inicializar el extractor
        extractor = orchestrator.extractor
        
        # Extraer subtítulos
        extraction_result = extractor.extract_subtitle_zip(
            url="file://" + zip_path,  # URL ficticia
            content_bytes=content_bytes
        )
        
        if extraction_result.get('success'):
            logger.info(f"Extracción exitosa, encontrados {len(extraction_result.get('subtitle_items', []))} ítems")
            
            # Crear un registro URL simulado para procesar
            url_record = {
                'url': 'file://' + zip_path,
                'url_hash': 'test_hash',
                'source': 'opensubtitles',
                'created_at': time.time(),
                'status': 'pending'
            }
            
            # Modificar el resultado de extracción para simular el procesamiento en _process_single_url
            orchestrator._processing_subtitle_items = extraction_result.get('subtitle_items', [])
            
            # Procesar la URL (esto debería agrupar los subtítulos)
            result = orchestrator._process_single_url(url_record)
            
            if result.get('success'):
                logger.info(f"Procesamiento exitoso: {result}")
                
                # Mostrar información sobre los archivos guardados
                if 'file_paths' in result:
                    for file_path in result.get('file_paths', []):
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
        else:
            logger.error(f"Error en la extracción: {extraction_result.get('error')}")
    
    finally:
        # Limpiar los archivos temporales
        try:
            shutil.rmtree(temp_dir)
            logger.info(f"Limpiados archivos temporales en {temp_dir}")
        except Exception as e:
            logger.warning(f"Error al limpiar archivos temporales: {e}")

if __name__ == "__main__":
    test_subtitle_grouping()
