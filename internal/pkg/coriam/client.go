package coriam

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/iam"
	log "github.com/sirupsen/logrus"
)

// IAMClient manages deploying IAM credentials for corral
type IAMClient struct {
	*iam.IAM
}

// AssumePolicyDocument is the policy document used in the role that coriam creates
const AssumePolicyDocument = `{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "",
      "Effect": "Allow",
      "Principal": {
        "Service": [
          "lambda.amazonaws.com"
        ]
      },
      "Action": "sts:AssumeRole"
    }
  ]
}`

// AttachPolicyDocument is the policy document used in the policy that coriam attaches to the created role
const AttachPolicyDocument = `{
    "Version": "2012-10-17",
    "Statement": [
		{
            "Effect": "Allow",
            "Action": [
                "logs:*"
            ],
            "Resource": "arn:aws:logs:*:*:*"
        },
        {
            "Effect": "Allow",
            "Action": [
                "lambda:InvokeFunction"
            ],
            "Resource": [
                "*"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "ec2:AttachNetworkInterface",
                "ec2:CreateNetworkInterface",
                "ec2:DeleteNetworkInterface",
                "ec2:DescribeInstances",
                "ec2:DescribeNetworkInterfaces",
                "ec2:DetachNetworkInterface",
                "ec2:ModifyNetworkInterfaceAttribute",
                "ec2:ResetNetworkInterfaceAttribute"
            ],
            "Resource": "*"
        },
        {
            "Effect": "Allow",
            "Action": [
                "s3:*"
            ],
            "Resource": "arn:aws:s3:::*"
        }
    ]
}`

const corralPolicyName = "corral-permissions"

// deployRole creates/updates the role with the given name so that it has the policy
// document that coriam defines (AssumePolicyDocument).
func (iamClient *IAMClient) deployRole(roleName string) (roleARN string, err error) {
	getParams := &iam.GetRoleInput{
		RoleName: aws.String(roleName),
	}
	exists, err := iamClient.GetRole(getParams)

	// Role already exists
	if exists != nil && err == nil {
		log.Debugf("IAM Role '%s' already exists", roleName)
		return *exists.Role.Arn, nil
	}

	createParams := &iam.CreateRoleInput{
		AssumeRolePolicyDocument: aws.String(AssumePolicyDocument),
		RoleName:                 aws.String(roleName),
	}
	log.Debugf("Creating IAM role '%s'", roleName)
	role, err := iamClient.CreateRole(createParams)
	if err != nil {
		return "", err
	}
	return *role.Role.Arn, err
}

// deployRole creates/updates the role with the given name so that it an
// attached inline policy that matches AttachPolicyDocument
func (iamClient *IAMClient) deployPolicy(roleName string) error {
	getParams := &iam.GetRolePolicyInput{
		RoleName:   aws.String(roleName),
		PolicyName: aws.String(corralPolicyName),
	}

	exists, err := iamClient.GetRolePolicy(getParams)

	// Policy already exists
	if exists != nil && err == nil {
		log.Debugf("Policy '%s' already exists", *exists.PolicyName)
		return nil
	}

	createParams := &iam.PutRolePolicyInput{
		PolicyName:     aws.String(corralPolicyName),
		PolicyDocument: aws.String(AttachPolicyDocument),
		RoleName:       aws.String(roleName),
	}

	log.Debugf("Creating policy '%s'", *createParams.PolicyName)
	_, err = iamClient.PutRolePolicy(createParams)
	return err
}

// DeployPermissions creates/updates IAM permissions for corral lambda functions.
// It creates/updates an IAM role and inline policy to allow corral lambda functions
// to access S3, invoke lambda functions, and write logs to CloudWatch.
func (iamClient *IAMClient) DeployPermissions(roleName string) (roleARN string, err error) {
	roleARN, err = iamClient.deployRole(roleName)
	if err != nil {
		return roleARN, err
	}

	err = iamClient.deployPolicy(roleName)

	return roleARN, err
}

// NewIAMClient initializes a new IAMClient
func NewIAMClient() *IAMClient {
	sess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))
	return &IAMClient{
		iam.New(sess),
	}
}
