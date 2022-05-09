import boto3
from sec_api import ExtractorApi, QueryApi

import os
import pickle


def add_to_s3(bucket, item_key, item_body):
    bucket.put_object(
        Key=item_key,
        Body=item_body
    )


def get_from_dynamo(table):
    response = table.scan()
    items = response["Items"]
    while "LastEvaluatedKey" in response:  # paginate due to 1MB return limit
        response = table.scan(ExclusiveStartKey=resposne["LastEvaluatedKey"])
        items.extend(response["Items"])

    return items


def update_status_dynamo(table, company_name, urls):
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


def get_10k_urls(query_api, table, items):
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


def get_10k_info(extractor_api, bucket, urls):
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
    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table(os.environ["DYNAMO_TABLE"])

    sec_query_api = QueryApi(api_key=os.environ["SEC_API_KEY"])
    sec_extractor_api = ExtractorApi(api_key=os.environ["SEC_API_KEY"])

    # make sure to specify task permission for ECS to access s3
    s3_resource = boto3.resource("s3")
    bucket = s3_resource.Bucket(os.environ["S3_BUCKET"])

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
    get_10k_info(sec_extractor_api, bucket, urls)
