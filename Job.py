from client.google.bigquery.GoogleBigQueryClient import GoogleBigQueryClient
from client.google.pubsub.GoogleCloudPubSubClient import GoogleCloudPubSubClient
from client.google.storage.GoogleCloudStorageClient import GoogleCloudStorageClient
from client.google.manage_resource.GoogleCloudManageResourceClient import GoogleCloudManageResourceClient

from proxy.google.GoogleCloudStorageProxy import GoogleCloudStorageProxy
from proxy.google.tracking.TrackingGoogleCloudStorageProxy import TrackingGoogleCloudStorageProxy
from proxy.google.logging.LoggingGoogleCloudStorageProxy import LoggingGoogleCloudStorageProxy
from proxy.google.monitoring.MonitoringGoogleCloudStorageProxy import MonitoringGoogleCloudStorageProxy
from proxy.google.security.SecurityGoogleCloudStorageProxy import SecurityGoogleCloudStorageProxy
from proxy.google.exception_handling.ExceptionHandlingGoogleCloudStorageProxy \
    import ExceptionHandlingGoogleCloudStorageProxy

from proxy.google.GoogleBigQueryProxy import GoogleBigQueryProxy
from proxy.google.tracking.TrackingGoogleBigQueryProxy import TrackingGoogleBigQueryProxy
from proxy.google.logging.LoggingGoogleBigQueryProxy import LoggingGoogleBigQueryProxy
from proxy.google.monitoring.MonitoringGoogleBigQueryProxy import MonitoringGoogleBigQueryProxy
from proxy.google.security.SecurityGoogleBigQueryProxy import SecurityGoogleBigQueryProxy
from proxy.google.exception_handling.ExceptionHandlingGoogleBigQueryProxy import ExceptionHandlingGoogleBigQueryProxy

from proxy.google.GoogleCloudPubSubProxy import GoogleCloudPubSubProxy
from proxy.google.tracking.TrackingGoogleCloudPubSubProxy import TrackingGoogleCloudPubSubProxy
from proxy.google.logging.LoggingGoogleCloudPubSubProxy import LoggingGoogleCloudPubSubProxy
from proxy.google.monitoring.MonitoringGoogleCloudPubSubProxy import MonitoringGoogleCloudPubSubProxy
from proxy.google.security.SecurityGoogleCloudPubSubProxy import SecurityGoogleCloudPubSubProxy
from proxy.google.exception_handling.ExceptionHandlingGoogleCloudPubSubProxy \
    import ExceptionHandlingGoogleCloudPubSubProxy

from service.DataService import DataService
from service.tracking.TrackingDataService import TrackingDataService
from service.logging.LoggingDataService import LoggingDataService
from service.monitoring.MonitoringDataService import MonitoringDataService
from service.security.SecurityDataService import SecurityDataService
from service.exception_handling.ExceptionHandlingDataService import ExceptionHandlingDataService

from service.StandardizeService import StandardizeService
from service.tracking.TrackingStandardizeService import TrackingStandardizeService
from service.logging.LoggingStandardizeService import LoggingStandardizeService
from service.monitoring.MonitoringStandardizeService import MonitoringStandardizeService
from service.security.SecurityStandardizeService import SecurityStandardizeService
from service.exception_handling.ExceptionHandlingStandardizeService import ExceptionHandlingStandardizeService

from service.DeduplicationService import DeduplicationService
from service.tracking.TrackingDeduplicationService import TrackingDeduplicationService
from service.logging.LoggingDeduplicationService import LoggingDeduplicationService
from service.monitoring.MonitoringDeduplicationService import MonitoringDeduplicationService
from service.security.SecurityDeduplicationService import SecurityDeduplicationService
from service.exception_handling.ExceptionHandlingDeduplicationService import ExceptionHandlingDeduplicationService

from service.QualityCheckService import QualityCheckService
from service.tracking.TrackingQualityCheckService import TrackingQualityCheckService
from service.logging.LoggingQualityCheckService import LoggingQualityCheckService
from service.monitoring.MonitoringQualityCheckService import MonitoringQualityCheckService
from service.security.SecurityQualityCheckService import SecurityQualityCheckService
from service.exception_handling.ExceptionHandlingQualityCheckService import ExceptionHandlingQualityCheckService

from service.ValidationService import ValidationService
from service.tracking.TrackingValidationService import TrackingValidationService
from service.logging.LoggingValidationService import LoggingValidationService
from service.monitoring.MonitoringValidationService import MonitoringValidationService
from service.security.SecurityValidationService import SecurityValidationService
from service.exception_handling.ExceptionHandlingValidationService import ExceptionHandlingValidationService

from service.TransformationService import TransformationService
from service.tracking.TrackingTransformationService import TrackingTransformationService
from service.logging.LoggingTransformationService import LoggingTransformationService
from service.monitoring.MonitoringTransformationService import MonitoringTransformationService
from service.security.SecurityTransformationService import SecurityTransformationService
from service.exception_handling.ExceptionHandlingTransformationService import ExceptionHandlingTransformationService

from service.ProcessingService import ProcessingService
from service.tracking.TrackingProcessingService import TrackingProcessingService
from service.logging.LoggingProcessingService import LoggingProcessingService
from service.monitoring.MonitoringProcessingService import MonitoringProcessingService
from service.security.SecurityProcessingService import SecurityProcessingService
from service.exception_handling.ExceptionHandlingProcessingService import ExceptionHandlingProcessingService


class Job(object):
    def __init__(self):
        self.__google_cloud_storage_client = GoogleCloudStorageClient()
        self.__google_cloud_storage_proxy = GoogleCloudStorageProxy(self.__google_cloud_storage_client)
        self.__google_cloud_storage_proxy = TrackingGoogleCloudStorageProxy(self.__google_cloud_storage_proxy)
        self.__google_cloud_storage_proxy = LoggingGoogleCloudStorageProxy(self.__google_cloud_storage_proxy)
        self.__google_cloud_storage_proxy = MonitoringGoogleCloudStorageProxy(self.__google_cloud_storage_proxy)
        self.__google_cloud_storage_proxy = SecurityGoogleCloudStorageProxy(self.__google_cloud_storage_proxy)
        self.__google_cloud_storage_proxy = ExceptionHandlingGoogleCloudStorageProxy(self.__google_cloud_storage_proxy)

        self.__google_bigquery_client = GoogleBigQueryClient()
        self.__google_cloud_manage_resource_client = GoogleCloudManageResourceClient()
        self.__google_bigquery_proxy = GoogleBigQueryProxy(self.__google_bigquery_client,
                                                           self.__google_cloud_manage_resource_client)
        self.__google_bigquery_proxy = TrackingGoogleBigQueryProxy(self.__google_bigquery_proxy)
        self.__google_bigquery_proxy = LoggingGoogleBigQueryProxy(self.__google_bigquery_proxy)
        self.__google_bigquery_proxy = MonitoringGoogleBigQueryProxy(self.__google_bigquery_proxy)
        self.__google_bigquery_proxy = SecurityGoogleBigQueryProxy(self.__google_bigquery_proxy)
        self.__google_bigquery_proxy = ExceptionHandlingGoogleBigQueryProxy(self.__google_bigquery_proxy)

        self.__google_cloud_pub_sub_client = GoogleCloudPubSubClient()
        self.__google_cloud_pub_sub_proxy = GoogleCloudPubSubProxy(self.__google_cloud_pub_sub_client)
        self.__google_cloud_pub_sub_proxy = TrackingGoogleCloudPubSubProxy(self.__google_cloud_pub_sub_proxy)
        self.__google_cloud_pub_sub_proxy = LoggingGoogleCloudPubSubProxy(self.__google_cloud_pub_sub_proxy)
        self.__google_cloud_pub_sub_proxy = MonitoringGoogleCloudPubSubProxy(self.__google_cloud_pub_sub_proxy)
        self.__google_cloud_pub_sub_proxy = SecurityGoogleCloudPubSubProxy(self.__google_cloud_pub_sub_proxy)
        self.__google_cloud_pub_sub_proxy = ExceptionHandlingGoogleCloudPubSubProxy(self.__google_cloud_pub_sub_proxy)

        self.__data_service = DataService(self.__google_cloud_storage_proxy, self.__google_bigquery_proxy,
                                          self.__google_cloud_pub_sub_proxy)
        self.__data_service = TrackingDataService(self.__data_service)
        self.__data_service = LoggingDataService(self.__data_service)
        self.__data_service = MonitoringDataService(self.__data_service)
        self.__data_service = SecurityDataService(self.__data_service)
        self.__data_service = ExceptionHandlingDataService(self.__data_service)

        self.__standardize_service = StandardizeService()
        self.__standardize_service = TrackingStandardizeService(self.__standardize_service)
        self.__standardize_service = LoggingStandardizeService(self.__standardize_service)
        self.__standardize_service = MonitoringStandardizeService(self.__standardize_service)
        self.__standardize_service = SecurityStandardizeService(self.__standardize_service)
        self.__standardize_service = ExceptionHandlingStandardizeService(self.__standardize_service)

        self.__deduplication_service = DeduplicationService()
        self.__deduplication_service = TrackingDeduplicationService(self.__deduplication_service)
        self.__deduplication_service = LoggingDeduplicationService(self.__deduplication_service)
        self.__deduplication_service = MonitoringDeduplicationService(self.__deduplication_service)
        self.__deduplication_service = SecurityDeduplicationService(self.__deduplication_service)
        self.__deduplication_service = ExceptionHandlingDeduplicationService(self.__deduplication_service)

        self.__quality_check_service = QualityCheckService()
        self.__quality_check_service = TrackingQualityCheckService(self.__quality_check_service)
        self.__quality_check_service = LoggingQualityCheckService(self.__quality_check_service)
        self.__quality_check_service = MonitoringQualityCheckService(self.__quality_check_service)
        self.__quality_check_service = SecurityQualityCheckService(self.__quality_check_service)
        self.__quality_check_service = ExceptionHandlingQualityCheckService(self.__quality_check_service)

        self.__validation_service = ValidationService()
        self.__validation_service = TrackingValidationService(self.__validation_service)
        self.__validation_service = LoggingValidationService(self.__validation_service)
        self.__validation_service = MonitoringValidationService(self.__validation_service)
        self.__validation_service = SecurityValidationService(self.__validation_service)
        self.__validation_service = ExceptionHandlingValidationService(self.__validation_service)

        self.__transformation_service = TransformationService()
        self.__transformation_service = TrackingTransformationService(self.__transformation_service)
        self.__transformation_service = LoggingTransformationService(self.__transformation_service)
        self.__transformation_service = MonitoringTransformationService(self.__transformation_service)
        self.__transformation_service = SecurityTransformationService(self.__transformation_service)
        self.__transformation_service = ExceptionHandlingTransformationService(self.__transformation_service)

        self.__processing_service = ProcessingService(
            self.__data_service,
            self.__standardize_service,
            self.__deduplication_service,
            self.__quality_check_service,
            self.__transformation_service,
            self.__validation_service
        )
        self.__processing_service = TrackingProcessingService(self.__processing_service)
        self.__processing_service = LoggingProcessingService(self.__processing_service)
        self.__processing_service = MonitoringProcessingService(self.__processing_service)
        self.__processing_service = SecurityProcessingService(self.__processing_service)
        self.__processing_service = ExceptionHandlingProcessingService(self.__processing_service)

    def main(self):
        self.__processing_service.execute()


if __name__ == "__main__":
    job = Job()
    job.main()
