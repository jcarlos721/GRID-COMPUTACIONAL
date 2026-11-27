"""Divide a imagem JSON em pacotes de linhas para processamento Sobel."""

import argparse
import json
from pathlib import Path


def export_chunks(json_path: Path, output_dir: Path, rows_per_chunk: int = 16) -> int:
    """Lê pixels row-major e grava pacotes JSON com faixas de linhas.

    Cada pacote informa as linhas centrais que o Worker deve devolver e inclui
    até uma linha adicional acima e abaixo. Essa margem fornece os vizinhos
    verticais necessários ao filtro Sobel 3×3 quando o pacote cobre a largura
    inteira da imagem.

    Args:
        json_path: JSON com width, height, channels e pixels.
        output_dir: Diretório onde os pacotes serão gravados.
        rows_per_chunk: Número de linhas centrais por pacote.

    Returns:
        Quantidade de pacotes criados.
    """
    with json_path.open("r", encoding="utf-8") as source:
        image = json.load(source)

    width = image["width"]
    height = image["height"]
    pixels = image["pixels"]
    output_dir.mkdir(parents=True, exist_ok=True)

    chunk_count = 0
    for row_start in range(0, height, rows_per_chunk):
        row_end = min(row_start + rows_per_chunk, height)
        input_row_start = max(0, row_start - 1)
        input_row_end = min(height, row_end + 1)

        pixel_start = input_row_start * width
        pixel_end = input_row_end * width
        chunk = {
            "task_id": chunk_count,
            "image_width": width,
            "image_height": height,
            "channels": image["channels"],
            "row_start": row_start,
            "row_end": row_end,
            "input_row_start": input_row_start,
            "input_row_end": input_row_end,
            "pixels": pixels[pixel_start:pixel_end],
        }

        chunk_path = output_dir / f"chunk_{chunk_count:04d}.json"
        with chunk_path.open("w", encoding="utf-8") as output:
            json.dump(chunk, output, separators=(",", ":"))

        print(
            f"{chunk_path}: linhas {row_start}:{row_end} "
            f"(entrada {input_row_start}:{input_row_end})"
        )
        chunk_count += 1

    return chunk_count


def main() -> None:
    """Lê opções CLI e cria os arquivos de pacote."""
    parser = argparse.ArgumentParser(description="Divide um JSON de imagem em chunks Sobel")
    parser.add_argument("json_path", type=Path, help="Caminho do JSON extraído")
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("src/result/chunks"),
        help="Diretório dos pacotes gerados",
    )
    parser.add_argument(
        "--rows-per-chunk",
        type=int,
        default=16,
        help="Linhas centrais em cada pacote (padrão: 16)",
    )
    args = parser.parse_args()

    count = export_chunks(args.json_path, args.output_dir, args.rows_per_chunk)
    print(f"Pacotes criados: {count}")


if __name__ == "__main__":
    main()
