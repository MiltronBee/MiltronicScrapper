#!/usr/bin/env python
"""
Formateador de subtítulos de OpenSubtitles.
Agrupa los diálogos de cada episodio en un solo archivo de texto,
con un salto de línea entre cada diálogo.
"""

import os
import sys
import logging
from pathlib import Path
import re
import shutil
from collections import defaultdict
import time
from corpus_scraper.orchestrator import Orchestrator

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

class OpenSubtitlesFormatter:
    def __init__(self, source_dir, output_dir=None):
        """
        Inicializa el formateador de OpenSubtitles.
        
        Args:
            source_dir: Directorio que contiene los archivos de subtítulos individuales
            output_dir: Directorio donde se guardarán los archivos combinados. Si es None,
                        se creará un subdirectorio 'combined' en el directorio fuente.
        """
        self.source_dir = Path(source_dir)
        
        if output_dir is None:
            self.output_dir = self.source_dir / "combined"
        else:
            self.output_dir = Path(output_dir)
            
        self.orchestrator = Orchestrator()
        
        # Estadísticas
        self.stats = {
            "episodios_procesados": 0,
            "archivos_combinados": 0,
            "total_dialogos": 0,
            "tiempo_inicio": time.time()
        }
        
        # Asegurarse de que el directorio de salida exista
        os.makedirs(self.output_dir, exist_ok=True)
        logger.info(f"Directorio de salida: {self.output_dir}")
    
    def _extract_episode_key(self, filename):
        """
        Extrae la clave del episodio de un nombre de archivo.
        Por ejemplo, de 'opensubtitles_20250626_1f18sweet_seymour_entry1.txt'
        extrae '1f18sweet_seymour'.
        
        Args:
            filename: Nombre del archivo de subtítulos
            
        Returns:
            Clave del episodio o None si no se puede extraer
        """
        match = re.match(r'opensubtitles_\d+_(.+?)_entry\d+\.txt', filename)
        if match:
            return match.group(1)
        return None
    
    def _group_files_by_episode(self):
        """
        Agrupa los archivos por episodio.
        
        Returns:
            Diccionario con las claves de episodio como claves y listas de archivos como valores
        """
        episode_files = defaultdict(list)
        
        for file_path in self.source_dir.glob("opensubtitles_*.txt"):
            if file_path.is_file():
                episode_key = self._extract_episode_key(file_path.name)
                if episode_key:
                    episode_files[episode_key].append(file_path)
        
        # Ordenar los archivos de cada episodio por número de entrada
        for episode_key in episode_files:
            episode_files[episode_key].sort(key=lambda x: int(re.search(r'entry(\d+)\.txt', x.name).group(1)))
        
        return episode_files
    
    def combine_episode_files(self, episode_key, file_paths):
        """
        Combina los archivos de un episodio en un solo archivo.
        
        Args:
            episode_key: Clave del episodio
            file_paths: Lista de rutas de archivos para este episodio
            
        Returns:
            Ruta del archivo combinado
        """
        output_file = self.output_dir / f"opensubtitles_{episode_key}_combined.txt"
        
        with open(output_file, "w", encoding="utf-8") as outfile:
            # Almacenar todos los diálogos
            dialogues = []
            
            # Leer cada archivo
            for file_path in file_paths:
                try:
                    with open(file_path, "r", encoding="utf-8") as infile:
                        content = infile.read().strip()
                        if content:  # Solo añadir si tiene contenido
                            dialogues.append(content)
                except Exception as e:
                    logger.error(f"Error al leer {file_path}: {e}")
            
            # Escribir todos los diálogos separados por un salto de línea
            outfile.write("\n\n".join(dialogues))
        
        return output_file
    
    def process_all(self):
        """
        Procesa todos los archivos de subtítulos.
        
        Returns:
            Estadísticas del procesamiento
        """
        start_time = time.time()
        
        # Agrupar archivos por episodio
        episode_files = self._group_files_by_episode()
        logger.info(f"Encontrados {len(episode_files)} episodios para procesar")
        
        # Procesar cada episodio
        for i, (episode_key, file_paths) in enumerate(episode_files.items(), 1):
            logger.info(f"Procesando episodio {i}/{len(episode_files)}: {episode_key}")
            
            # Combinar los archivos del episodio
            output_file = self.combine_episode_files(episode_key, file_paths)
            
            # Actualizar estadísticas
            self.stats["episodios_procesados"] += 1
            self.stats["archivos_combinados"] += len(file_paths)
            self.stats["total_dialogos"] += len(file_paths)
            
            # Reportar progreso
            if i % 5 == 0 or i == len(episode_files):
                progress = i / len(episode_files) * 100
                logger.info(f"Progreso: {progress:.1f}% ({i}/{len(episode_files)})")
        
        # Calcular tiempo total
        self.stats["tiempo_total"] = time.time() - start_time
        self.stats["tiempo_formato"] = f"{self.stats['tiempo_total']:.1f} segundos"
        
        # Añadir estadísticas finales
        logger.info(f"Procesamiento completado en {self.stats['tiempo_formato']}:")
        logger.info(f"- Episodios procesados: {self.stats['episodios_procesados']}")
        logger.info(f"- Archivos combinados: {self.stats['archivos_combinados']}")
        logger.info(f"- Total diálogos: {self.stats['total_dialogos']}")
        
        # Enviar reporte a Discord
        self._report_to_discord()
        
        return self.stats
    
    def _report_to_discord(self):
        """Envía un reporte a Discord usando el orquestador"""
        try:
            title = "Formateo de OpenSubtitles completado"
            description = (f"**Episodios procesados:** {self.stats['episodios_procesados']}\n"
                          f"**Archivos combinados:** {self.stats['archivos_combinados']}\n"
                          f"**Total diálogos:** {self.stats['total_dialogos']}\n"
                          f"**Tiempo:** {self.stats['tiempo_formato']}\n\n"
                          f"Guardado en `{self.output_dir}`")
            
            # Usar el método de reporte a Discord del orquestador
            self.orchestrator._report_to_discord(title, description)
            logger.info("Reporte enviado a Discord")
        except Exception as e:
            logger.error(f"Error al enviar reporte a Discord: {e}")

def main():
    """Función principal"""
    # Obtener directorio de origen
    source_dir = "/mnt/c/Users/vulca/proyects/MiltronicScrapper/code/data/corpus_raw/opensubtitles"
    
    if len(sys.argv) > 1:
        source_dir = sys.argv[1]
    
    # Verificar que el directorio de origen exista
    if not os.path.exists(source_dir):
        print(f"Error: El directorio de origen {source_dir} no existe")
        sys.exit(1)
    
    # Crear el directorio de destino
    output_dir = os.path.join(source_dir, "combined")
    
    # Inicializar y ejecutar el formateador
    formatter = OpenSubtitlesFormatter(source_dir, output_dir)
    stats = formatter.process_all()
    
    print(f"Procesamiento completado:")
    print(f"- Episodios procesados: {stats['episodios_procesados']}")
    print(f"- Archivos combinados: {stats['archivos_combinados']}")
    print(f"- Total diálogos: {stats['total_dialogos']}")
    print(f"- Tiempo: {stats['tiempo_formato']}")
    print(f"- Archivos guardados en: {output_dir}")

if __name__ == "__main__":
    main()
