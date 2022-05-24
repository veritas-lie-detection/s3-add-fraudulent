import os
import pickle
from typing import Dict, List

import boto3
from sec_api import ExtractorApi, QueryApi



def add_to_s3(bucket, item_key: str, item_body) -> None:
    """Adds a writable object to s3.

    This function is copied from s3-add-nonfraud. This function (along with others in this package)
        should be moved to a separate helper repository and imported.

    Args:
        bucket: The s3 bucket to add a file to.
        item_key: The name of the file being added.
        item_body: Information to write to the bucket.
    """
    bucket.put_object(
        Key=item_key,
        Body=item_body
    )


def get_from_dynamo(table) -> List[Dict]:
    """Gets all items from a DynamoDB table.

    This function is copied from s3-add-nonfraud. This function (along with others in this package)
        should be moved to a separate helper repository and imported.

    Args:
        table: The DynamoDB table name.

    Returns:
        All items in the DynamoDB.
    """
    response = table.scan()
    items = response["Items"]
    while "LastEvaluatedKey" in response:  # paginate due to 1MB return limit
        response = table.scan(ExclusiveStartKey=resposne["LastEvaluatedKey"])
        items.extend(response["Items"])

    return items


def update_status_dynamo(table, company_name: str, urls: List[str]) -> Dict:
    """Set DynamoDB item to scraped.

    Args:
        table: The DynamoDB table object.
        company_name: Name of company to set scraped (primary key of table).
        urls: The list of URLs representing AAERs associated with the company. 
    """
    for url in urls:
        response = table.update_item(
            Key={
                "company_name": company_name,
                "url": url,
            },
            UpdateExpression="set scraped = :s",
            ExpressionAttributeValues={
                ":s": True
            },
            ReturnValues="UPDATED_NEW"
        )

    return response


def get_10k_urls(query_api, table, items: Dict) -> List[Dict[str, str]]:
    """Gets the urls of fraudulent 10-Ks.

    Args:
        query_api: SEC API to get a company's filing records.
        table: The DynamoDB table object.
        items: Information about fraudulent companies.

    Returns:
        List of information about fraudulent documents.
    """
    urls = []
    for key in items:
        query = {
            "query": {
                "query_string": {
                    "query": "cik: \"" + key + \
                        "\" AND filedAt:{" + str(int(items[key]["start_year"])) + \
                        "-01-01 TO " + str(int(items[key]["end_year"])) + \
                        "-12-31} AND formType:\"10-K\" AND documentFormatFiles.type: \"10-K\""
                }
            }
        }

        filings = query_api.get_filings(query)

        found = False
        if "filings" in filings and len(filings["filings"]) > 0:
            for filing in filings["filings"]:
                for document in filing["documentFormatFiles"]:
                    if document["type"].lower() == "10-k":
                        urls.append(
                            {
                                "url": document["documentUrl"],
                                "cik": key,
                                "year": filing["filedAt"][:4],
                            }
                        )
                        found = True
                        break
        if found:
            update_status_dynamo(table, items[key]["company_name"], items[key]["urls"])
            print("Successfully scraped.")
        else:
            print(key + " never found.")
    
    return urls


def add_10k_info(extractor_api, bucket, urls: List[Dict[str, str]]) -> None:
    """Adds 10-K documents to the S3 bucket.

    This function is copied from s3-add-nonfraud. This function (along with others in this package)
        should be moved to a separate helper repository and imported.

    Args:
        extractor_api: SEC API to get 10-K from their respective links.
        bucket: The S3 bucket to add the files to.
        urls: A list of dictionaries containing information about a 10-K document.
    """
    for url_object in urls:
        url = url_object["url"]
        item = {
            "url": url,
            "1A": extractor_api.get_section(url, "1A", "text"),
            "7": extractor_api.get_section(url, "7", "text"),
            "7A": extractor_api.get_section(url, "7A", "text"),
        }
        item_pickle = pickle.dumps(item)
        add_to_s3(bucket, "fraudulent/{}/{}.pkl".format(url_object["cik"], url_object["year"]), item_pickle)
        print("Add attempted.")


if __name__ == "__main__":
    # initialize resources
    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table(os.environ["DYNAMO_TABLE"])

    sec_query_api = QueryApi(api_key=os.environ["SEC_API_KEY"])
    sec_extractor_api = ExtractorApi(api_key=os.environ["SEC_API_KEY"])

    # make sure to specify task permission for ECS to access s3
    s3_resource = boto3.resource("s3")
    bucket = s3_resource.Bucket(os.environ["S3_BUCKET"])

    # main process
    fraud_company_info = get_from_dynamo(table)
    time_ranges = {}
    for info in fraud_company_info:
        if ("scraped" in info and info["scraped"]) or info["year_start"] > info["year_end"] or not info["contains_21c"]:
            continue
        if info["year_start"] == info["year_end"] and info["month_end"] - info["month_start"] < 6:
            print("Skipping company due to fraudulent activity being < 6 months.")
            continue
        if info["cik"] not in time_ranges:
            time_ranges[info["cik"]] = {
                "start_year": info["year_start"],
                "end_year": info["year_end"],
                "company_name": info["company_name"],
                "urls": [info["url"]],
            }
        else:
            time_ranges[info["cik"]]["urls"].append(info["url"])
            if time_ranges[info["cik"]]["start_year"] > info["year_start"]:
                time_ranges[info["cik"]]["start_year"] = info["year_start"]
            if time_ranges[info["cik"]]["end_year"] > info["year_end"]:
                time_ranges[info["cik"]]["end_year"] = info["year_end"]
    
    urls = get_10k_urls(sec_query_api, table, time_ranges)
    add_10k_info(sec_extractor_api, bucket, urls)
