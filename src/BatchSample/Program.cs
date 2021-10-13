using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Microsoft.Azure.Batch;
using Microsoft.Azure.Batch.Auth;
using Microsoft.Azure.Batch.Common;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;

namespace BatchSample
{
    public static class Program
    {
        // Batch account credentials
        private const string BatchAccountName = "<type yours>";
        private const string BatchAccountKey = "<type yours>";
        private const string BatchAccountUrl = "<type yours>";

        // Storage account credentials
        private const string StorageAccountName = "<type yours>";
        private const string StorageAccountKey = "<type yours>";

        // Batch resource settings
        private const string PoolId = "netcoreconf-pool";
        private const string JobId = "netcoreconf-job";
        private const int PoolNodeCount = 2;
        private const string PoolVmSize = "STANDARD_A1_v2";
        
        const string InputBlobContainerName = "input";

        static readonly List<string> FilePathsToProcess = new List<string>
        {
            "stock0.csv",
            "stock1.csv",
            "stock2.csv"
        };

        private static async Task Main()
        {
            EnsureCredentials();

            try
            {
                Console.WriteLine("Sample start: {0}", DateTime.Now);
                Console.WriteLine();

                var blobClient = CreateCloudBlobClient(StorageAccountName, StorageAccountKey);
                var container = await GetContainerReference(blobClient);

                var inputFiles = UploadFiles(blobClient);

                using (var batchClient = CreateBatchClient())
                {
                    
                    CreateBatchPool(batchClient);
                    var jobId=CreateBatchJob(batchClient);
                    var tasks = CreateTasks(inputFiles);
                    
                    await batchClient.JobOperations.AddTaskAsync(jobId, tasks);

                    WaitJobFinished(batchClient);

                    Console.WriteLine();
                    Console.WriteLine("Printing task output...");

                    foreach (var task in batchClient.JobOperations.ListTasks(JobId))
                    {
                        Console.WriteLine("Task: {0}", task.Id);
                        Console.WriteLine("Node: {0}", task.ComputeNodeInformation.ComputeNodeId);
                        Console.WriteLine("Standard out:");
                        Console.WriteLine(await (await task.GetNodeFileAsync(Constants.StandardOutFileName)).ReadAsStringAsync());
                    }

                    await CleanUpAsync(container, batchClient);
                }
            }
            finally
            {
                Console.WriteLine();
                Console.WriteLine("Sample complete, hit ENTER to exit...");
                Console.ReadLine();
            }
        }

        private static async Task CleanUpAsync(CloudBlobContainer container, BatchClient batchClient)
        {
            await container.DeleteIfExistsAsync();
            Console.WriteLine($"Container [{InputBlobContainerName}] deleted.");
            
            await batchClient.JobOperations.DeleteJobAsync(JobId);
            Console.WriteLine($"Job [{JobId}] deleted.");
            
            await batchClient.PoolOperations.DeletePoolAsync(PoolId);
            Console.WriteLine($"Pool [{PoolId}] deleted.");
        }

        private static void WaitJobFinished(BatchClient batchClient)
        {
            // Monitor task success/failure, specifying a maximum amount of time to wait for the tasks to complete.

            TimeSpan timeout = TimeSpan.FromMinutes(30);
            Console.WriteLine("Monitoring all tasks for 'Completed' state, timeout in {0}...", timeout);

            IEnumerable<CloudTask> addedTasks = batchClient.JobOperations.ListTasks(JobId);

            batchClient.Utilities.CreateTaskStateMonitor().WaitAll(addedTasks, TaskState.Completed, timeout);

            Console.WriteLine("All tasks reached state Completed.");
        }

        private static IEnumerable<CloudTask> CreateTasks(List<ResourceFile> inputFiles)
        {
            Console.WriteLine("Adding {0} tasks to job [{1}]...", inputFiles.Count, JobId);

            var tasks = new List<CloudTask>();

            for (int i = 0; i < inputFiles.Count; i++)
            {
                var taskId = $"Task{i}";
                var inputFilename = inputFiles[i].FilePath;
                //var taskCommandLine = $"cmd /c type {inputFilename}"; //Use this for win VM's
                var taskCommandLine = $"/bin/bash -c 'cat {inputFilename}'";

                var task = new CloudTask(taskId, taskCommandLine)
                {
                    ResourceFiles = new List<ResourceFile> { inputFiles[i] }
                };
                tasks.Add(task);
            }

            return tasks;
        }

        private static string CreateBatchJob(BatchClient batchClient)
        {
            Console.WriteLine("Creating job [{0}]...", JobId);

            try
            {
                var job = batchClient.JobOperations.CreateJob();
                job.Id = JobId;
                job.PoolInformation = new PoolInformation { PoolId = PoolId };

                job.Commit();
            }
            catch (BatchException be)
            {
                // Accept the specific error code JobExists as that is expected if the job already exists
                if (be.RequestInformation?.BatchError?.Code == BatchErrorCodeStrings.JobExists)
                {
                    Console.WriteLine("The job {0} already existed when we tried to create it", JobId);
                }
                else
                {
                    throw; // Any other exception is unexpected
                }
            }
            return JobId;
        }

        private static BatchClient CreateBatchClient()
        {
            var cred = new BatchSharedKeyCredentials(BatchAccountUrl, BatchAccountName, BatchAccountKey);
            return BatchClient.Open(cred);
        }

        private static List<ResourceFile> UploadFiles(CloudBlobClient blobClient)
        {
            // Upload the data files to Azure Storage.
            var inputFiles = new List<ResourceFile>();

            foreach (var filePath in FilePathsToProcess)
            {
                inputFiles.Add(UploadFileToContainer(blobClient, InputBlobContainerName, filePath));
            }

            return inputFiles;
        }

        private static async Task<CloudBlobContainer> GetContainerReference(CloudBlobClient blobClient)
        {
            var container = blobClient.GetContainerReference(InputBlobContainerName);
            await container.CreateIfNotExistsAsync();
            return container;
        }

        private static void EnsureCredentials()
        {
            if (string.IsNullOrEmpty(BatchAccountName) ||
                string.IsNullOrEmpty(BatchAccountKey) ||
                string.IsNullOrEmpty(BatchAccountUrl) ||
                string.IsNullOrEmpty(StorageAccountName) ||
                string.IsNullOrEmpty(StorageAccountKey))
            {
                throw new InvalidOperationException(
                    "One or more account credential strings have not been populated. Please ensure that your Batch and Storage account credentials have been specified.");
            }
        }

        private static void CreateBatchPool(BatchClient batchClient)
        {
            try
            {
                Console.WriteLine("Creating pool [{0}]...", PoolId);

                // Create a Windows Server image, VM configuration, Batch pool
                var imageReference = CreateImageReference();
                var vmConfiguration = CreateVirtualMachineConfiguration(imageReference);

                var pool = batchClient.PoolOperations.CreatePool(
                    poolId: PoolId,
                    targetDedicatedComputeNodes: PoolNodeCount,
                    virtualMachineSize: PoolVmSize,
                    virtualMachineConfiguration: vmConfiguration);

                pool.Commit();
            }
            catch (BatchException be)
            {
                // Accept the specific error code PoolExists as that is expected if the pool already exists
                if (be.RequestInformation?.BatchError?.Code == BatchErrorCodeStrings.PoolExists)
                {
                    Console.WriteLine("The pool {0} already existed when we tried to create it", PoolId);
                }
                else
                {
                    throw; // Any other exception is unexpected
                }
            }
        }

        //private static VirtualMachineConfiguration CreateVirtualMachineConfiguration(ImageReference imageReference)
        //{
        //    return new VirtualMachineConfiguration(
        //        imageReference: imageReference,
        //        nodeAgentSkuId: "batch.node.windows amd64");
        //}

        //private static ImageReference CreateImageReference()
        //{
        //    return new ImageReference(
        //        publisher: "MicrosoftWindowsServer",
        //        offer: "WindowsServer",
        //        sku: "2016-datacenter-smalldisk",
        //        version: "latest");
        //}

        private static VirtualMachineConfiguration CreateVirtualMachineConfiguration(ImageReference imageReference)
        {
            return new VirtualMachineConfiguration(
                imageReference: imageReference,
                nodeAgentSkuId: "batch.node.ubuntu 18.04");
        }

        private static ImageReference CreateImageReference()
        {
            return new ImageReference(
                publisher: "Canonical",
                offer: "UbuntuServer",
                sku: "18.04-LTS",
                version: "latest");
        }

        private static CloudBlobClient CreateCloudBlobClient(string storageAccountName, string storageAccountKey)
        {
            string storageConnectionString =
                $"DefaultEndpointsProtocol=https;AccountName={storageAccountName};AccountKey={storageAccountKey}";

            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(storageConnectionString);
            CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();

            return blobClient;
        }

        private static ResourceFile UploadFileToContainer(CloudBlobClient blobClient, string containerName, string filePath)
        {
            Console.WriteLine("Uploading file {0} to container [{1}]...", filePath, containerName);

            string blobName = Path.GetFileName(filePath);

            filePath = Path.Combine(Environment.CurrentDirectory, filePath);

            var container = blobClient.GetContainerReference(containerName);
            var blobData = container.GetBlockBlobReference(blobName);
            blobData.UploadFromFileAsync(filePath).Wait();

            // Set the expiry time and permissions for the blob shared access signature. In this case, no start time is specified,
            // so the shared access signature becomes valid immediately
            var sasConstraints = new SharedAccessBlobPolicy
            {
                SharedAccessExpiryTime = DateTime.UtcNow.AddHours(2),
                Permissions = SharedAccessBlobPermissions.Read
            };

            // Construct the SAS URL for blob
            var sasBlobToken = blobData.GetSharedAccessSignature(sasConstraints);
            var blobSasUri = $"{blobData.Uri}{sasBlobToken}";

            return new ResourceFile(blobSasUri, blobName);
        }
    }
}
