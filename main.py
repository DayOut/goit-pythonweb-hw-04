import argparse
import asyncio
import logging
import shutil
from pathlib import Path
import os

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
semaphore = asyncio.Semaphore(100)


async def copy_file(file_path: Path, source_dir: Path, output_dir: Path):
    async with semaphore:
        extension = "".join(file_path.suffixes).lstrip('.') or 'no_extension'
        relative_path = file_path.relative_to(source_dir)
        target_dir = output_dir / extension / relative_path.parent

        target_file = target_dir / file_path.name

        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, lambda: os.makedirs(target_dir, exist_ok=True))

        counter = 1
        while target_file.exists():
            target_file = target_dir / f"{file_path.stem}_{counter}{file_path.suffix}"
            counter += 1

        try:
            await loop.run_in_executor(None, shutil.copy2, file_path, target_file)
            logging.info(f"Копійовано: {file_path} -> {target_file}")
        except Exception as e:
            logging.error(f"Помилка копіювання {file_path}: {e}")


async def read_folder(source_dir: Path, output_dir: Path):
    loop = asyncio.get_running_loop()

    file_paths = await loop.run_in_executor(None, lambda: list(source_dir.rglob("*.*")))

    tasks = [copy_file(file_path, source_dir, output_dir) for file_path in file_paths]
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

    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, lambda: os.makedirs(output_folder, exist_ok=True))

    await read_folder(source_folder, output_folder)


if __name__ == "__main__":
    asyncio.run(main())
