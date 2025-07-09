import json
import os
import re
from typing import Any, Dict, List

import boto3
from aws_utils import (
    extract_textract_data,
    generate_embedding,
)
from botocore.config import Config
from langchain_aws import BedrockEmbeddings
from opensearch_utils import bulk_add_to_opensearch, create_document
from table_tools import extract_table_content, get_table_base64_from_pdf
from textractor.data.text_linearization_config import TextLinearizationConfig

config = Config(read_timeout=600, retries=dict(max_attempts=5))

REGION_NAME = os.getenv("AWS_REGION")
INDEX_NAME = os.getenv("INDEX_NAME")
EMBEDDINGS_MODEL_ID = os.getenv("EMBEDDINGS_MODEL_ID")

bedrock_runtime = boto3.client(
    service_name="bedrock-runtime", config=config, region_name=REGION_NAME
)
s3 = boto3.client("s3", region_name=REGION_NAME)
embeddings_client = BedrockEmbeddings(
    model_id=EMBEDDINGS_MODEL_ID, region_name=REGION_NAME
)


def strip_newline(cell: Any) -> str:
    """Remove newline characters from a cell value."""
    return str(cell).strip()


def sub_header_content_splitter(string: str) -> List[str]:
    """Split content by XML tags and return relevant segments."""
    pattern = re.compile(r"<<[^>]+>>")
    segments = re.split(pattern, string)
    result = []
    for segment in segments:
        if segment.strip():
            if (
                "<header>" not in segment
                and "<list>" not in segment
                and "<table>" not in segment
            ):
                segment = [x.strip() for x in segment.split("\n") if x.strip()]
                result.extend(segment)
            else:
                result.append(segment)
    return result


def split_list_items_(items: str) -> List[str]:
    """Split a string into a list of items, handling nested lists."""
    parts = re.split("(<<list>><list>|</list><</list>>)", items)
    output = []

    inside_list = False
    list_item = ""

    for p in parts:
        if p == "<<list>><list>":
            inside_list = True
            list_item = p
        elif p == "</list><</list>>":
            inside_list = False
            list_item += p
            output.append(list_item)
            list_item = ""
        elif inside_list:
            list_item += p.strip()
        else:
            output.extend(p.split("\n"))
    return output


def process_document(document, local_pdf_path: str) -> [Dict, Dict]:
    """Process a document from textract, extract different items."""

    config = TextLinearizationConfig(
        hide_figure_layout=False,
        title_prefix="<titles><<title>><title>",
        title_suffix="</title><</title>>",
        hide_header_layout=True,
        section_header_prefix="<headers><<header>><header>",
        section_header_suffix="</header><</header>>",
        table_prefix="<tables><table>",
        table_suffix="</table>",
        list_layout_prefix="<<list>><list>",
        list_layout_suffix="</list><</list>>",
        hide_footer_layout=True,
        hide_page_num_layout=True,
    )

    document_holder = {}
    table_page = {}
    count = 0
    # Loop through each page in the document
    for ids, page in enumerate(document.pages):
        table_count = len(
            [
                word
                for word in page.get_text(config=config).split()
                if "<tables><table>" in word
            ]
        )
        assert table_count == len(page.tables)
        content = page.get_text(config=config).split("<tables>")
        document_holder[ids] = []
        for idx, item in enumerate(content):
            if "<table>" in item:
                table = document.tables[count]

                bounding_box = table.bbox

                table_pg_number = table.page

                table_base64 = get_table_base64_from_pdf(
                    local_pdf_path, table_pg_number, bounding_box
                )

                if ids in table_page:
                    table_page[ids].append(table_base64)
                else:
                    table_page[ids] = [table_base64]

                # Extract table data and remaining content
                pattern = re.compile(r"<table>(.*?)(</table>)", re.DOTALL)
                data = item
                table_match = re.search(pattern, data)
                remaining_content = (
                    data[table_match.end() :] if table_match else data
                )

                content[idx] = (
                    f"<<table>><table>{table_base64}</table><</table>>"
                )
                count += 1

                if "<<list>>" in remaining_content:
                    output = split_list_items_(remaining_content)
                    output = [x.strip() for x in output if x.strip()]
                    document_holder[ids].extend([content[idx]] + output)
                else:
                    document_holder[ids].extend(
                        [content[idx]]
                        + [
                            x.strip()
                            for x in remaining_content.split("\n")
                            if x.strip()
                        ]
                    )
            else:
                if "<<list>>" in item and "<table>" not in item:
                    output = split_list_items_(item)
                    output = [x.strip() for x in output if x.strip()]
                    document_holder[ids].extend(output)
                else:
                    document_holder[ids].extend(
                        [x.strip() for x in item.split("\n") if x.strip()]
                    )

    page_mapping = {}
    current_page = 1

    for page in document.pages:
        page_content = page.get_text(config=config)
        page_mapping[current_page] = page_content
        current_page += 1

    flattened_list = [
        item for sublist in document_holder.values() for item in sublist
    ]
    result = "\n".join(flattened_list)
    header_split = result.split("<titles>")

    return header_split, page_mapping


def chunk_document(header_split, file, BUCKET, page_mapping):
    """Document chunking"""
    max_words = 200
    chunks = {}
    table_header_dict = {}
    chunk_header_mapping = {}
    list_header_dict = {}

    def find_page_number(content):
        for page_num, page_content in page_mapping.items():
            if content in page_content:
                return page_num
        return None

    for title_ids, items in enumerate(header_split):
        title_chunks = []
        current_chunk = {"content": [], "metadata": {}}
        num_words = 0
        table_header_dict[title_ids] = {}
        chunk_header_mapping[title_ids] = {}
        list_header_dict[title_ids] = {}
        chunk_counter = 0
        last_known_page = 1

        doc_id = os.path.basename(file)

        for item_ids, item in enumerate(items.split("<headers>")):
            lines = sub_header_content_splitter(item)
            SECTION_HEADER = None
            TITLES = None
            num_words = 0
            for ids_line, line in enumerate(lines):
                if line.strip():
                    page_number = find_page_number(line)
                    if page_number:
                        last_known_page = page_number
                    current_chunk["metadata"]["page"] = last_known_page

                    if "<title>" in line:
                        TITLES = re.findall(r"<title>(.*?)</title>", line)[
                            0
                        ].strip()
                        line = TITLES
                        current_chunk["metadata"]["title"] = TITLES
                        if (
                            re.sub(r"<[^>]+>", "", "".join(lines)).strip()
                            == TITLES
                        ):
                            chunk_header_mapping[title_ids][chunk_counter] = (
                                lines
                            )
                            chunk_counter += 1
                    if "<header>" in line:
                        SECTION_HEADER = re.findall(
                            r"<header>(.*?)</header>", line
                        )[0].strip()
                        line = SECTION_HEADER
                        current_chunk["metadata"]["section_header"] = (
                            SECTION_HEADER
                        )
                        first_header_portion = True
                    next_num_words = num_words + len(re.findall(r"\w+", line))

                    if "<table>" in line or "<list>" in line:
                        current_chunk["metadata"]["page"] = last_known_page

                    if "<table>" not in line and "<list>" not in line:
                        if (
                            next_num_words > max_words
                            and "".join(current_chunk["content"]).strip()
                            != SECTION_HEADER
                            and current_chunk["content"]
                            and "".join(current_chunk["content"]).strip()
                            != TITLES
                        ):
                            if SECTION_HEADER:
                                if first_header_portion:
                                    first_header_portion = False
                                else:
                                    current_chunk["content"].insert(
                                        0, SECTION_HEADER.strip()
                                    )

                            title_chunks.append(current_chunk)
                            chunk_header_mapping[title_ids][chunk_counter] = (
                                lines
                            )

                            current_chunk = {"content": [], "metadata": {}}
                            if SECTION_HEADER:
                                current_chunk["metadata"]["section_header"] = (
                                    SECTION_HEADER
                                )
                            if TITLES:
                                current_chunk["metadata"]["title"] = TITLES
                            num_words = 0
                            chunk_counter += 1

                        current_chunk["content"].append(line)
                        num_words += len(re.findall(r"\w+", line))

                    if "<table>" in line:
                        line_index = lines.index(line)
                        if (
                            line_index != 0
                            and "<table>" not in lines[line_index - 1]
                            and "<list>" not in lines[line_index - 1]
                        ):
                            header = (
                                lines[line_index - 1]
                                .replace("<header>", "")
                                .replace("</header>", "")
                            )
                        else:
                            header = ""

                        table_base64 = re.search(
                            r"<table>(.*?)</table>", line
                        ).group(1)

                        current_chunk["content"].append(
                            f"<table>{header}<base64>{table_base64}</base64></table>"
                        )

                        num_words = 0

                    if "<list>" in line:
                        line_index = lines.index(line)
                        if (
                            line_index != 0
                            and "<table>" not in lines[line_index - 1]
                            and "<list>" not in lines[line_index - 1]
                        ):
                            header = (
                                lines[line_index - 1]
                                .replace("<header>", "")
                                .replace("</header>", "")
                            )
                        else:
                            header = ""
                        list_pattern = re.compile(
                            r"<list>(.*?)(?:</list>|$)", re.DOTALL
                        )
                        list_match = re.search(list_pattern, line)
                        list_ = list_match.group(1)
                        list_lines = list_.split("\n")

                        curr_chunk = []
                        words = len(re.findall(r"\w+", str(current_chunk)))
                        for lyst_item in list_lines:
                            curr_chunk.append(lyst_item)
                            words += len(re.findall(r"\w+", lyst_item))
                            if words >= max_words:
                                if [
                                    x
                                    for x in list_header_dict[title_ids]
                                    if chunk_counter == x
                                ]:
                                    list_header_dict[title_ids][
                                        chunk_counter
                                    ].extend([header] + [list_])
                                else:
                                    list_header_dict[title_ids][
                                        chunk_counter
                                    ] = [header] + [list_]
                                words = 0
                                list_chunk = "\n".join(curr_chunk)
                                if header:
                                    if (
                                        current_chunk["content"]
                                        and current_chunk["content"][-1]
                                        .strip()
                                        .lower()
                                        == header.strip().lower()
                                    ):
                                        current_chunk["content"].pop()
                                    if (
                                        SECTION_HEADER
                                        and SECTION_HEADER.lower().strip()
                                        != header.lower().strip()
                                    ):
                                        if first_header_portion:
                                            first_header_portion = False
                                        else:
                                            current_chunk["content"].insert(
                                                0, SECTION_HEADER.strip()
                                            )

                                    current_chunk["content"].extend(
                                        [
                                            header.strip() + ":"
                                            if not header.strip().endswith(":")
                                            else header.strip()
                                        ]
                                        + [list_chunk]
                                    )
                                    title_chunks.append(current_chunk)

                                else:
                                    if SECTION_HEADER:
                                        if first_header_portion:
                                            first_header_portion = False
                                        else:
                                            current_chunk["content"].insert(
                                                0, SECTION_HEADER.strip()
                                            )

                                    current_chunk["content"].extend(
                                        [list_chunk]
                                    )
                                    title_chunks.append(current_chunk)
                                chunk_header_mapping[title_ids][
                                    chunk_counter
                                ] = lines
                                chunk_counter += 1
                                num_words = 0
                                current_chunk = {"content": [], "metadata": {}}
                                curr_chunk = []
                        if curr_chunk and lines.index(line) == len(lines) - 1:
                            list_chunk = "\n".join(curr_chunk)
                            if [
                                x
                                for x in list_header_dict[title_ids]
                                if chunk_counter == x
                            ]:
                                list_header_dict[title_ids][
                                    chunk_counter
                                ].extend([header] + [list_])
                            else:
                                list_header_dict[title_ids][chunk_counter] = [
                                    header
                                ] + [list_]
                            if header:
                                if (
                                    current_chunk["content"]
                                    and current_chunk["content"][-1]
                                    .strip()
                                    .lower()
                                    == header.strip().lower()
                                ):
                                    current_chunk["content"].pop()
                                if (
                                    SECTION_HEADER
                                    and SECTION_HEADER.lower().strip()
                                    != header.lower().strip()
                                ):
                                    if first_header_portion:
                                        first_header_portion = False
                                    else:
                                        current_chunk["content"].insert(
                                            0, SECTION_HEADER.strip()
                                        )
                                current_chunk["content"].extend(
                                    [
                                        header.strip() + ":"
                                        if not header.strip().endswith(":")
                                        else header.strip()
                                    ]
                                    + [list_chunk]
                                )
                                title_chunks.append(current_chunk)
                            else:
                                if SECTION_HEADER:
                                    if first_header_portion:
                                        first_header_portion = False
                                    else:
                                        current_chunk["content"].insert(
                                            0, SECTION_HEADER.strip()
                                        )
                                current_chunk["content"].extend([list_chunk])
                                title_chunks.append(current_chunk)
                            chunk_header_mapping[title_ids][chunk_counter] = (
                                lines
                            )
                            chunk_counter += 1
                            num_words = 0
                            current_chunk = {"content": [], "metadata": {}}
                        elif curr_chunk and lines.index(line) != len(lines) - 1:
                            list_chunk = "\n".join(curr_chunk)
                            if [
                                x
                                for x in list_header_dict[title_ids]
                                if chunk_counter == x
                            ]:
                                list_header_dict[title_ids][
                                    chunk_counter
                                ].extend([header] + [list_])
                            else:
                                list_header_dict[title_ids][chunk_counter] = [
                                    header
                                ] + [list_]
                            if header:
                                if (
                                    current_chunk["content"]
                                    and current_chunk["content"][-1]
                                    .strip()
                                    .lower()
                                    == header.strip().lower()
                                ):
                                    current_chunk["content"].pop()
                                current_chunk["content"].extend(
                                    [
                                        header.strip() + ":"
                                        if not header.strip().endswith(":")
                                        else header.strip()
                                    ]
                                    + [list_chunk]
                                )
                            else:
                                current_chunk["content"].extend([list_chunk])
                            num_words = words

            if (
                current_chunk["content"]
                and "".join(current_chunk["content"]).strip() != SECTION_HEADER
                and "".join(current_chunk["content"]).strip() != TITLES
            ):
                if SECTION_HEADER:
                    if first_header_portion:
                        first_header_portion = False
                    else:
                        current_chunk["content"].insert(
                            0, SECTION_HEADER.strip()
                        )
                title_chunks.append(current_chunk)
                chunk_header_mapping[title_ids][chunk_counter] = lines
                current_chunk = {"content": [], "metadata": {}}
                chunk_counter += 1
        if current_chunk["content"]:
            title_chunks.append(current_chunk)
            chunk_header_mapping[title_ids][chunk_counter] = lines
        chunks[title_ids] = title_chunks

    for x in chunk_header_mapping:
        if chunk_header_mapping[x]:
            try:
                title_pattern = re.compile(
                    r"<title>(.*?)(?:</title>|$)", re.DOTALL
                )
                title_match = re.search(
                    title_pattern, chunk_header_mapping[x][0][0]
                )
                title_ = title_match.group(1) if title_match else ""
            except:
                continue

    with open(f"/tmp/{doc_id}.json", "w") as f:
        json.dump(chunk_header_mapping, f)
    s3.upload_file(
        f"/tmp/{doc_id}.json", BUCKET, f"chunked_jsons/{doc_id}.json"
    )
    os.remove(f"/tmp/{doc_id}.json")

    doc = {
        "chunks": chunks,
        "chunk_header_mapping": chunk_header_mapping,
        "table_header_dict": table_header_dict,
        "list_header_dict": list_header_dict,
        "doc_id": doc_id,
    }

    return doc


def process_chunk(chunk, last_known_page):
    if isinstance(chunk, dict):
        passage_chunk = "\n".join(chunk["content"])
        page_number = (
            int(chunk["metadata"].get("page", last_known_page))
            if chunk["metadata"]
            else last_known_page
        )
    elif isinstance(chunk, list):
        passage_chunk = "\n".join(chunk)
        page_number = last_known_page
    else:
        raise ValueError(f"Unexpected chunk type: {type(chunk)}")

    return passage_chunk.replace("<title>", "").replace(
        "</title>", ""
    ), page_number


def insert_document(doc: dict, metadata: Dict[str, str]):
    chunks = doc["chunks"]
    list_header_dict = doc["list_header_dict"]
    doc_id = doc["doc_id"]

    last_known_page = 1
    documents_to_index = []

    for ids, chunkks in chunks.items():
        if not chunkks:
            continue
        for chunk_ids, chunk in enumerate(chunkks):
            try:
                passage_chunk, last_known_page = process_chunk(
                    chunk, last_known_page
                )
                passage_chunk, table_base64, table_context = (
                    extract_table_content(passage_chunk)
                )
                passage_chunk = re.sub(r"<[^>]+>", "", passage_chunk)

                if passage_chunk.strip() or table_base64:
                    try:
                        embedding = (
                            generate_embedding(embeddings_client, passage_chunk)
                            if passage_chunk.strip()
                            else None
                        )

                        # Only proceed if we have a valid embedding
                        if embedding is not None:
                            lists = "\n".join(
                                list_header_dict.get(ids, {}).get(chunk_ids, [])
                            )

                            combined_metadata = {
                                "doc_id": doc_id,
                                "table_context": table_context,
                                "table_base64": table_base64,
                                "list": lists,
                                "page_number": last_known_page,
                            }
                            combined_metadata.update(metadata)

                            document = create_document(
                                passage=passage_chunk,
                                embedding=embedding,
                                type="pdf",
                                metadata=combined_metadata,
                            )

                            documents_to_index.append(document)
                        else:
                            print(
                                f"Skipping chunk due to missing embedding. Passage: {passage_chunk[:100]}..."
                            )
                    except Exception as e:
                        print(f"Error generating embedding: {e}")

            except Exception as e:
                print(f"Error processing chunk: {e}")

    if documents_to_index:
        success = bulk_add_to_opensearch(documents_to_index)
        if success:
            print(
                f"Successfully bulk indexed {len(documents_to_index)} documents"
            )
            return True
        else:
            print("Failed to bulk index documents")
            return False
    else:
        print("No documents to index")
        return False


def update_cache_file(bucket, s3_uri):
    """
    Add a successfully processed S3 URI to the cache file.
    """
    try:
        s3_client = boto3.client("s3")

        # Get existing cache contents
        try:
            response = s3_client.get_object(Bucket=bucket, Key="cache_file.txt")
            content = response["Body"].read().decode("utf-8").strip()
            if content:
                existing_cache = set(
                    line.strip() for line in content.split("\n") if line.strip()
                )
            else:
                existing_cache = set()
        except Exception:
            # If file doesn't exist or other error, create empty set
            existing_cache = set()

        # Add new URI
        if s3_uri not in existing_cache:
            existing_cache.add(s3_uri)

            # Write back to S3
            cache_content = "\n".join(sorted(existing_cache))
            s3_client.put_object(
                Bucket=bucket, Key="cache_file.txt", Body=cache_content
            )

            print(f"Added to cache: {s3_uri}")
        else:
            print(f"Already in cache: {s3_uri}")

    except Exception as e:
        print(f"Error updating cache file: {str(e)}")


def get_bucket_from_s3_uri(s3_uri):
    """Extract bucket name from S3 URI."""
    return s3_uri.replace("s3://", "").split("/")[0]


def parse_s3_uri(s3_uri):
    if not s3_uri.startswith("s3://"):
        raise ValueError("Invalid S3 URI")

    s3_path = s3_uri[5:]

    bucket_name, *key_parts = s3_path.split("/", 1)
    file_key = key_parts[0] if key_parts else ""

    return bucket_name, file_key


def process_pdf(s3_uri, metadata, bucket_name, s3_file_path):
    print(f"Processing {os.path.basename(s3_file_path)}")

    document, local_pdf_path = extract_textract_data(s3, s3_uri, bucket_name)

    header_split, page_mapping = process_document(document, local_pdf_path)

    doc_chunks = chunk_document(
        header_split, s3_file_path, bucket_name, page_mapping
    )

    success = insert_document(doc_chunks, metadata)

    if success:
        print(
            f"Processed and added {os.path.basename(s3_file_path)} to OpenSearch successfully."
        )

        # Only add to cache after successful OpenSearch insertion
        bucket = get_bucket_from_s3_uri(s3_uri)
        update_cache_file(bucket, s3_uri)

        return {
            "statusCode": 200,
            "body": json.dumps(
                f"Processed and added {os.path.basename(s3_file_path)} to OpenSearch successfully."
            ),
        }
    else:
        print(f"Failed to process {os.path.basename(s3_file_path)}")
        return {
            "statusCode": 500,
            "body": json.dumps(
                f"Failed to process {os.path.basename(s3_file_path)}"
            ),
        }


def get_s3_metadata(s3_uri):
    """Extract metadata from S3 object custom metadata."""
    s3_client = boto3.client("s3")

    bucket, key = parse_s3_uri(s3_uri)

    try:
        response = s3_client.head_object(Bucket=bucket, Key=key)
        # Extract custom metadata (x-amz-meta-* headers)
        custom_metadata = {
            k.replace("x-amz-meta-", ""): v.replace("=", "-").replace("?", "")
            if isinstance(v, str)
            else v
            for k, v in response.get("Metadata", {}).items()
        }
        return custom_metadata
    except Exception as e:
        print(f"Error fetching S3 metadata for {s3_uri}: {str(e)}")
        return {}


def lambda_handler(event, context):
    """Lambda handler for PDF processing"""
    try:
        print("Processing PDF document")
        print(f"Event: {json.dumps(event)}")

        s3_uri = event["s3_uri"]
        bucket_name, s3_file_path = parse_s3_uri(s3_uri)
        metadata = get_s3_metadata(s3_uri)

        return process_pdf(s3_uri, metadata, bucket_name, s3_file_path)

    except Exception as e:
        print(f"Error processing PDF: {str(e)}")
        return {
            "statusCode": 500,
            "body": json.dumps(f"Error processing PDF: {str(e)}"),
        }
