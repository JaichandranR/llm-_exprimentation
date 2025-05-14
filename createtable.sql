CREATE TABLE apitables_prototype.verum_v12_prototype.gtmetrics_cp6104_aws_resources (
  iri varchar,
  resourceid varchar,
  resourcetype varchar,
  creationtime varchar,
  lob varchar,
  arn varchar,
  region varchar,
  modulename varchar,
  moduleversion varchar,
  moduleinstanceid varchar,
  resourcename varchar,
  publishedby varchar,
  lastloadtime varchar,
  configurationitemstatus varchar,
  configurationitemcapturetime varchar,
  provisionedby varchar,
  belongstoawsaccount varchar
  -- Remove `post_body` for now to avoid oversized rows
)
WITH (
  api_additional_headers = '{}',
  api_request_retry_settings = '{ "retry":true,"attempts":5,"delaySeconds":15 }',
  api_request_timeout_secs = 60,
  api_response_header_accept = 'application/json',
  cache_cache_table = false,
  cache_ttl_secs = 3600,
  jsonpath_record_selector = '$.data.getBulkAWSResources[*]',
  page_number_settings = '{
    "shouldUpdatePageNumber": true,
    "pageNumReplacementVar": "offset",
    "firstPageNum": "0",
    "nextPageSelector": ""
  }',
  page_size_settings = '{
    "shouldUpdatePageSize": false,
    "pageSize": 500,
    "pageSizeReplacementVar": ""
  }',
  paging_settings = '{
    "pagingType": "OFFSET_RECORDS",
    "hasLastPageField": false
  }',
  post_pattern = '{"offset": ${offset}}',
  request_method = 'POST',
  return_format = 'JSON',
  target_uri_pattern = 'https://tpr-mock-api.apps-dev.na-9z.gap.jpmchase.net/verum_v12_aws_resources',
  use_validation = false,
  validation_error_on_failure = false
);
