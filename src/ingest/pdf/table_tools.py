import base64
import io
import re

import pdfplumber


def get_table_base64_from_pdf(
    local_pdf_path, page_number, bounding_box, resolution=300
):
    with pdfplumber.open(local_pdf_path) as pdf:
        # Get the specific page
        page = pdf.pages[page_number - 1]

        # Convert the page to an image
        img = page.to_image(resolution)  # You can adjust the resolution as needed

        # Get the dimensions of the image
        img_width, img_height = img.original.size

        # Calculate the crop box based on the bounding box
        left = int(bounding_box.x * img_width)
        top = int(bounding_box.y * img_height)
        right = int((bounding_box.x + bounding_box.width) * img_width)
        bottom = int((bounding_box.y + bounding_box.height) * img_height)

        # Convert the image to a PIL Image
        pil_img = img.original

        # Crop the image
        cropped_img = pil_img.crop((left, top, right, bottom))

        # Convert the cropped image to bytes
        buffered = io.BytesIO()
        cropped_img.save(buffered, format="PNG")

        # Encode the image to base64
        img_str = base64.b64encode(buffered.getvalue()).decode()

        return img_str


def extract_table_content(passage_chunk):
    table_base64 = ""
    table_context = ""
    table_match = re.search(r"<table>(.*?)</table>", passage_chunk, re.DOTALL)
    if table_match:
        table_content = table_match.group(1)
        base64_match = re.search(r"<base64>(.*?)</base64>", table_content, re.DOTALL)
        if base64_match:
            table_base64 = base64_match.group(1)
            table_context = re.sub(
                r"<base64>.*?</base64>", "", table_content, flags=re.DOTALL
            ).strip()
        else:
            table_context = table_content.strip()

        passage_chunk = re.sub(
            r"<table>.*?</table>", "", passage_chunk, flags=re.DOTALL
        )

    return passage_chunk, table_base64, table_context
