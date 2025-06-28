#!/usr/bin/env python
"""
Procesador de datasets de audio en español.
Extrae los textos normalizados de archivos parquet y los guarda en formato .txt
"""

import os
import sys
import logging
import pandas as pd
from pathlib import Path
import time
import json
from corpus_scraper.orchestrator import Orchestrator

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

class DatasetProcessor:
    def __init__(self, source_dir, dest_base_dir):
        """
        Inicializa el procesador de Librivox.
        
        Args:
            source_dir: Directorio que contiene los archivos parquet
            dest_base_dir: Directorio base donde se guardarán los textos extraídos
        """
        self.source_dir = Path(source_dir)
        self.dest_base_dir = Path(dest_base_dir)
        self.orchestrator = Orchestrator()
        
        # Estadísticas
        self.stats = {
            "archivos_procesados": 0,
            "textos_extraidos": 0,
            "total_tokens": 0,
            "tiempo_inicio": time.time()
        }
    
    def _create_destination_dir(self, source_name):
        """Crea el directorio de destino con el prefijo ciempiess_"""
        # Obtener solo el último componente del nombre del directorio
        base_name = os.path.basename(source_name)
        dest_dir_name = f"ciempiess_{base_name}"
        dest_dir = self.dest_base_dir / dest_dir_name
        os.makedirs(dest_dir, exist_ok=True)
        logger.info(f"Creado directorio de destino: {dest_dir}")
        return dest_dir
        
    def process_file(self, file_path, dest_dir):
        """
        Procesa un archivo parquet y extrae los textos normalizados.
        
        Args:
            file_path: Ruta al archivo parquet
            dest_dir: Directorio donde guardar los textos extraídos
        
        Returns:
            Diccionario con estadísticas del procesamiento
        """
        try:
            logger.info(f"Procesando archivo: {file_path}")
            
            # Leer el archivo parquet
            df = pd.read_parquet(file_path)
            
            if "normalized_text" not in df.columns:
                raise ValueError(f"El archivo {file_path} no contiene la columna 'normalized_text'")
            
            # Obtener los textos normalizados
            texts = df["normalized_text"].tolist()
            
            # Nombre del archivo de salida
            output_file = dest_dir / f"{os.path.basename(file_path).replace('.parquet', '.txt')}"
            
            # Guardar los textos con un doble salto de línea entre cada texto (línea en blanco)
            with open(output_file, "w", encoding="utf-8") as f:
                f.write("\n\n".join(texts))
            
            # Actualizar estadísticas
            tokens_count = sum(len(text.split()) for text in texts)
            
            return {
                "textos_extraidos": len(texts),
                "tokens": tokens_count,
                "output_file": str(output_file)
            }
            
        except Exception as e:
            logger.error(f"Error al procesar {file_path}: {e}")
            return {"textos_extraidos": 0, "tokens": 0, "error": str(e)}
    
    def process_all(self):
        """
        Procesa todos los archivos parquet en el directorio.
        
        Returns:
            Diccionario con estadísticas del procesamiento
        """
        start_time = time.time()
        
        # Crear directorio de destino
        dest_dir = self._create_destination_dir(self.source_dir)
        
        # Buscar todos los archivos parquet
        parquet_files = list(self.source_dir.glob("**/*.parquet"))
        logger.info(f"Encontrados {len(parquet_files)} archivos parquet para procesar")
        
        # Procesar cada archivo
        for i, file_path in enumerate(parquet_files):
            logger.info(f"Procesando archivo {i+1}/{len(parquet_files)}: {file_path.name}")
            result = self.process_file(file_path, dest_dir)
            
            # Actualizar estadísticas
            self.stats["archivos_procesados"] += 1
            self.stats["textos_extraidos"] += result.get("textos_extraidos", 0)
            self.stats["total_tokens"] += result.get("tokens", 0)
            
            # Reportar progreso
            if (i+1) % 5 == 0 or (i+1) == len(parquet_files):
                progress = (i+1) / len(parquet_files) * 100
                logger.info(f"Progreso: {progress:.1f}% ({i+1}/{len(parquet_files)})")
        
        # Calcular tiempo total
        self.stats["tiempo_total"] = time.time() - start_time
        self.stats["tiempo_formato"] = f"{self.stats['tiempo_total']:.1f} segundos"
        
        # Añadir estadísticas finales
        logger.info(f"Procesamiento completado en {self.stats['tiempo_formato']}:")
        logger.info(f"- Archivos procesados: {self.stats['archivos_procesados']}")
        logger.info(f"- Textos extraídos: {self.stats['textos_extraidos']}")
        logger.info(f"- Total tokens: {self.stats['total_tokens']}")
        
        # Enviar reporte a Discord
        self._report_to_discord()
        
        return self.stats
    
    def _report_to_discord(self):
        """Envía un reporte a Discord usando el orquestador"""
        try:
            dataset_name = os.path.basename(self.source_dir)
            title = f"Procesamiento de {dataset_name} completado"
            description = (f"**Archivos procesados:** {self.stats['archivos_procesados']}\n"
                          f"**Textos extraídos:** {self.stats['textos_extraidos']}\n"
                          f"**Total tokens:** {self.stats['total_tokens']:,}\n"
                          f"**Tiempo:** {self.stats['tiempo_formato']}\n\n"
                          f"Guardado en `{os.path.basename(self.dest_base_dir)}/ciempiess_{os.path.basename(self.source_dir)}`")
            
            # Usar el método de reporte a Discord del orquestador
            self.orchestrator._report_to_discord(title, description)
            logger.info("Reporte enviado a Discord")
        except Exception as e:
            logger.error(f"Error al enviar reporte a Discord: {e}")

def main():
    """Función principal"""
    if len(sys.argv) < 2:
        print("Uso: python dataset_processor.py [directorio1] [directorio2] ...")
        print("Los directorios deben contener archivos .parquet con la columna 'normalized_text'")
        print("Datasets conocidos: librivox_spanish, tele_con_ciencia, voxforge_spanish, tedx_spanish")
        sys.exit(1)
    
    # Obtener directorio de destino
    dest_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "corpus_raw")
    os.makedirs(dest_dir, exist_ok=True)
    
    # Procesar cada directorio especificado
    for source_dir in sys.argv[1:]:
        if not os.path.exists(source_dir):
            print(f"Error: El directorio {source_dir} no existe. Saltando...")
            continue
            
        print(f"\nProcesando dataset: {os.path.basename(source_dir)}")
        
        # Inicializar y ejecutar el procesador para este directorio
        processor = DatasetProcessor(source_dir, dest_dir)
        stats = processor.process_all()
        
        print(f"Dataset {os.path.basename(source_dir)} completado:")
        print(f"- Archivos: {stats['archivos_procesados']}")
        print(f"- Textos: {stats['textos_extraidos']}")
        print(f"- Tokens: {stats['total_tokens']:,}")
        print(f"- Tiempo: {stats['tiempo_formato']}")

if __name__ == "__main__":
    # Renombrar el archivo a un nombre más genérico
    if os.path.basename(__file__) == "librivox_processor.py":
        print("Nota: Este script ha sido actualizado para procesar múltiples tipos de datasets.")
        print("Se recomienda renombrarlo a 'dataset_processor.py' para reflejar su funcionalidad más genérica.")
    main()
