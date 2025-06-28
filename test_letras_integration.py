#!/usr/bin/env python3
"""
Test script para verificar la integración de las soluciones de codificación UTF-8
para letras.com en el sistema principal.
"""
import os
import sys
import logging
import yaml
import hashlib
from datetime import datetime
from pathlib import Path

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("test_letras_integration")

# Importar componentes del sistema principal
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from corpus_scraper.letras_scraper import LetrasScraper
from corpus_scraper.saver import Saver

def load_config(config_path="config.yaml"):
    """Cargar archivo de configuración."""
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)
    except Exception as e:
        logger.error(f"Error cargando config: {e}")
        return None

def test_letras_url(url, config):
    """Probar la extracción y guardado de una URL de letras.com usando los componentes actualizados."""
    try:
        logger.info(f"Probando URL: {url}")
        
        # Inicializar componentes
        scraper = LetrasScraper()
        saver = Saver(config.get("storage", {}))
        
        # Extraer letras
        logger.info("Extrayendo contenido...")
        result = scraper.extract_lyrics(url)
        
        if 'error' in result and result['error']:
            logger.error(f"Error en extracción: {result['error']}")
            return False
            
        if 'lyrics' not in result or not result['lyrics']:
            logger.error("No se encontró contenido de letras")
            return False
            
        lyrics = result['lyrics']
        word_count = result.get('lyrics_word_count', 0)
        sentence_count = result.get('lyrics_sentence_count', 0)
        
        logger.info(f"Extracción exitosa: {word_count} palabras, {sentence_count} oraciones")
        
        # Verificar que sea texto y no binario
        try:
            # Esto fallará si lyrics contiene datos binarios
            lyrics_str = str(lyrics)
            if not isinstance(lyrics_str, str):
                logger.error(f"El contenido extraído no es una cadena de texto válida: {type(lyrics)}")
                return False
                
            logger.info(f"Verificación de texto: OK - Longitud: {len(lyrics_str)}")
        except Exception as e:
            logger.error(f"Error verificando el contenido: {e}")
            return False
            
        # Guardar el contenido
        logger.info("Guardando contenido...")
        save_result = saver.save_text(
            content=lyrics,
            source_name="letras_com",
            url=url,
            metadata={
                "word_count": word_count,
                "sentence_count": sentence_count
            }
        )
        
        if not save_result.get('saved'):
            logger.error(f"Error guardando archivo: {save_result.get('error')}")
            return False
            
        file_path = save_result.get('file_path')
        logger.info(f"Archivo guardado en: {file_path}")
        
        # Verificar el archivo guardado
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                saved_content = f.read()
                
            if saved_content == lyrics:
                logger.info("✅ Verificación exitosa: el contenido guardado coincide con el extraído")
            else:
                logger.error("❌ Verificación fallida: el contenido guardado NO coincide con el extraído")
                return False
                
        except UnicodeDecodeError as e:
            logger.error(f"❌ Error de codificación al leer el archivo guardado: {e}")
            return False
        except Exception as e:
            logger.error(f"❌ Error verificando el archivo guardado: {e}")
            return False
            
        # Mostrar las primeras líneas del contenido
        preview_lines = lyrics.split('\n')[:5]
        logger.info("Vista previa del contenido:")
        for line in preview_lines:
            logger.info(f"  > {line}")
            
        return True
            
    except Exception as e:
        logger.error(f"Error durante la prueba: {e}")
        return False

def main():
    # Cargar configuración
    config = load_config()
    if not config:
        logger.error("No se pudo cargar la configuración")
        return 1
        
    # Lista de URLs de prueba
    test_urls = [
        "https://www.letras.com/natanael-cano/sin-ti/",
        "https://www.letras.com/peso-pluma/humo-part-chencho-corleone/"
    ]
    
    # Probar cada URL
    success_count = 0
    for url in test_urls:
        logger.info("-" * 60)
        success = test_letras_url(url, config)
        logger.info(f"Resultado: {'✅ ÉXITO' if success else '❌ FALLIDO'}")
        if success:
            success_count += 1
    
    # Resumen
    logger.info("=" * 60)
    logger.info(f"Prueba completada: {success_count}/{len(test_urls)} URLs procesadas correctamente")
    logger.info("=" * 60)
    
    return 0 if success_count == len(test_urls) else 1

if __name__ == "__main__":
    sys.exit(main())
