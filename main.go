package main

import (
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/rekognition"
	"github.com/aws/aws-sdk-go/service/s3"
)

func main() {
	sess := session.Must(session.NewSession(&aws.Config{
		Region: aws.String("us-west-1"),
	}))

	svc := s3.New(sess)
	rekog := rekognition.New(sess)

	// 1. List objects in the ratcam bucket.
	input := &s3.ListObjectsV2Input{
		Bucket: aws.String("ratcam"),
	}
	result, err := svc.ListObjectsV2(input)
	if err != nil {
		fmt.Println("Error in listing objects:", err)
		return
	}

	for _, item := range result.Contents {
		key := *item.Key

		// Skip if processed
		if strings.HasPrefix(key, "processed/") {
			continue
		}

		// 2. Detect faces using Rekognition.
		rekogInput := &rekognition.DetectFacesInput{
			Image: &rekognition.Image{
				S3Object: &rekognition.S3Object{
					Bucket: aws.String("ratcam"),
					Name:   aws.String(key),
				},
			},
		}
		rekogResult, err := rekog.DetectFaces(rekogInput)
		if err != nil {
			fmt.Printf("Error in detecting faces for %s: %s\n", key, err)
			continue
		}

		// 3. Move to the appropriate directory.
		var newKey string
		if len(rekogResult.FaceDetails) > 0 {
			newKey = fmt.Sprintf("processed/positive/%s", key)
		} else {
			newKey = fmt.Sprintf("processed/negative/%s", key)
		}

		// Copy object to new location
		_, err = svc.CopyObject(&s3.CopyObjectInput{
			Bucket:     aws.String("ratcam"),
			CopySource: aws.String("ratcam/" + key),
			Key:        aws.String(newKey),
		})
		if err != nil {
			fmt.Printf("Error in copying %s: %s\n", key, err)
			continue
		}

		// Delete original object
		_, err = svc.DeleteObject(&s3.DeleteObjectInput{
			Bucket: aws.String("ratcam"),
			Key:    aws.String(key),
		})
		if err != nil {
			fmt.Printf("Error in deleting %s: %s\n", key, err)
		} else {
			fmt.Printf("Processed %s\n", key)
		}
	}
}

