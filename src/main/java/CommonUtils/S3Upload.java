package CommonUtils;

import PojoClasses.UserFilePOJO;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;

import java.io.File;

public class S3Upload {

    public static void s3UploadFile(UserFilePOJO filePath){
        Regions clientRegion = Regions.DEFAULT_REGION;
        String bucketName = "bidgely-amerenres-nonprodqa";
        String stringObjKeyName = "";
        String[] fileNameTemp  = filePath.getUserfile_abs_path().split("/");
        String fileObjKeyName = fileNameTemp[fileNameTemp.length-1];
        String fileName = filePath.getUserfile_abs_path();

        try

        {
            //This code expects that you have AWS credentials set up per:
            // https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/setup-credentials.html
            AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
                    .withRegion(clientRegion)
                    .build();

            // Upload a text string as a new object.
            //s3Client.putObject(bucketName, stringObjKeyName, "Uploaded String Object");

            // Upload a file as a new object with ContentType and title specified.
            PutObjectRequest request = new PutObjectRequest(bucketName, fileObjKeyName, new File(fileName));
            ObjectMetadata metadata = new ObjectMetadata();
            metadata.addUserMetadata("utility_file_name", fileObjKeyName);
            request.setMetadata(metadata);
            s3Client.putObject(request);
        } catch(
                AmazonServiceException e)

        {
            // The call was transmitted successfully, but Amazon S3 couldn't process
            // it, so it returned an error response.
            e.printStackTrace();
        } catch(
                SdkClientException e)

        {
            // Amazon S3 couldn't be contacted for a response, or the client
            // couldn't parse the response from Amazon S3.
            e.printStackTrace();
        }
    }

}
