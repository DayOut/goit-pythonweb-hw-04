import argparse
import asyncio
import aiofiles
import aiofiles.os
import aiofiles.ospath
import os
from pathlib import Path
import shutil
import logging

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
semaphore = asyncio.Semaphore(100)


async def copy_file(file_path: Path, output_dir: Path):
    async with semaphore:
        extension = file_path.suffix[1:] or 'no_extension'
        target_dir = output_dir / extension

        target_file = target_dir / file_path.name

        try:
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, lambda: os.makedirs(target_dir, exist_ok=True))
            await loop.run_in_executor(None, shutil.copy2, file_path, target_file)
            logging.info(f"Копійовано: {file_path} -> {target_file}")
        except Exception as e:
            logging.error(f"Помилка копіювання {file_path}: {e}")


async def read_folder(source_dir: Path, output_dir: Path):
    tasks = []

    for root, _, files in os.walk(source_dir):
        for file in files:
            full_path = Path(root) / file
            tasks.append(copy_file(full_path, output_dir))

    await asyncio.gather(*tasks)


async def main():
    parser = argparse.ArgumentParser(description="Асинхронне сортування файлів за розширенням.")
    parser.add_argument("source_folder", type=Path, help="Шлях до вихідної папки")
    parser.add_argument("output_folder", type=Path, help="Шлях до папки призначення")
    args = parser.parse_args()

    source_folder = args.source_folder.resolve()
    output_folder = args.output_folder.resolve()

    if not source_folder.exists():
        logging.error(f"Вихідна папка не існує: {source_folder}")
        return

    if not output_folder.exists():
        await aiofiles.os.mkdir(output_folder)

    await read_folder(source_folder, output_folder)

if __name__ == "__main__":
    asyncio.run(main())
