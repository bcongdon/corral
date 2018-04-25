package coriam

import (
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/iam"
	"github.com/aws/aws-sdk-go/service/iam/iamiface"
	"github.com/stretchr/testify/assert"
)

type iamMock struct {
	iamiface.IAMAPI
	roleExists                          bool
	policyExists                        bool
	attachRolePolicyDocument            string
	assumeRolePolicyDocument            string
	capturedGetRoleInput                *iam.GetRoleInput
	capturedCreateRoleInput             *iam.CreateRoleInput
	capturedUpdateAssumeRolePolicyInput *iam.UpdateAssumeRolePolicyInput
	capturedGetRolePolicyInput          *iam.GetRolePolicyInput
	capturedPutRolePolicyInput          *iam.PutRolePolicyInput
	capturedDeleteRolePolicyInput       *iam.DeleteRolePolicyInput
	capturedDeleteRoleInput             *iam.DeleteRoleInput
}

func (i *iamMock) GetRole(input *iam.GetRoleInput) (*iam.GetRoleOutput, error) {
	i.capturedGetRoleInput = input
	if !i.roleExists {
		return nil, nil
	}
	return &iam.GetRoleOutput{
		Role: &iam.Role{
			RoleName: input.RoleName,
			Arn:      aws.String("testARN"),
			AssumeRolePolicyDocument: aws.String(i.assumeRolePolicyDocument),
		},
	}, nil
}

func (i *iamMock) UpdateAssumeRolePolicy(input *iam.UpdateAssumeRolePolicyInput) (*iam.UpdateAssumeRolePolicyOutput, error) {
	i.capturedUpdateAssumeRolePolicyInput = input
	return nil, nil
}

func (i *iamMock) CreateRole(input *iam.CreateRoleInput) (*iam.CreateRoleOutput, error) {
	i.capturedCreateRoleInput = input
	return &iam.CreateRoleOutput{
		Role: &iam.Role{
			Arn: aws.String("testARN"),
		},
	}, nil
}

func (i *iamMock) GetRolePolicy(input *iam.GetRolePolicyInput) (*iam.GetRolePolicyOutput, error) {
	i.capturedGetRolePolicyInput = input
	if !i.policyExists {
		return nil, nil
	}
	return &iam.GetRolePolicyOutput{
		RoleName:       input.RoleName,
		PolicyDocument: aws.String(i.attachRolePolicyDocument),
	}, nil
}

func (i *iamMock) PutRolePolicy(input *iam.PutRolePolicyInput) (*iam.PutRolePolicyOutput, error) {
	i.capturedPutRolePolicyInput = input
	return nil, nil
}

func (i *iamMock) DeleteRolePolicy(input *iam.DeleteRolePolicyInput) (*iam.DeleteRolePolicyOutput, error) {
	i.capturedDeleteRolePolicyInput = input
	return nil, nil
}

func (i *iamMock) DeleteRole(input *iam.DeleteRoleInput) (*iam.DeleteRoleOutput, error) {
	i.capturedDeleteRoleInput = input
	return nil, nil
}

func TestCreateRole(t *testing.T) {
	mock := &iamMock{
		roleExists: false,
	}
	client := IAMClient{mock}

	arn, err := client.deployRole("role")
	assert.Nil(t, err)
	assert.Equal(t, "testARN", arn)
	assert.Equal(t, "role", *mock.capturedCreateRoleInput.RoleName)
	assert.Equal(t, AssumePolicyDocument, *mock.capturedCreateRoleInput.AssumeRolePolicyDocument)
}

func TestUpdateRole(t *testing.T) {
	mock := &iamMock{
		roleExists:               true,
		assumeRolePolicyDocument: "incorrect document",
	}
	client := IAMClient{mock}

	arn, err := client.deployRole("role")
	assert.Nil(t, err)
	assert.Equal(t, "testARN", arn)
	assert.Equal(t, "role", *mock.capturedUpdateAssumeRolePolicyInput.RoleName)
	assert.Equal(t, AssumePolicyDocument, *mock.capturedUpdateAssumeRolePolicyInput.PolicyDocument)
}

func TestCreatePolicy(t *testing.T) {
	mock := &iamMock{
		policyExists: false,
	}
	client := IAMClient{mock}

	err := client.deployPolicy("role")
	assert.Nil(t, err)
	assert.Equal(t, "role", *mock.capturedGetRolePolicyInput.RoleName)
	assert.Equal(t, "corral-permissions", *mock.capturedGetRolePolicyInput.PolicyName)
	assert.Equal(t, AttachPolicyDocument, *mock.capturedPutRolePolicyInput.PolicyDocument)
}

func TestUpdatePolicy(t *testing.T) {
	mock := &iamMock{
		policyExists:             true,
		attachRolePolicyDocument: "incorrect document",
	}
	client := IAMClient{mock}

	err := client.deployPolicy("role")
	assert.Nil(t, err)
	assert.Equal(t, "role", *mock.capturedGetRolePolicyInput.RoleName)
	assert.Equal(t, "corral-permissions", *mock.capturedGetRolePolicyInput.PolicyName)
	assert.Equal(t, AttachPolicyDocument, *mock.capturedPutRolePolicyInput.PolicyDocument)
}

func TestDeployPermissions(t *testing.T) {
	mock := &iamMock{
		roleExists:   false,
		policyExists: false,
	}
	client := IAMClient{mock}

	arn, err := client.DeployPermissions("role")
	assert.Nil(t, err)
	assert.Equal(t, "testARN", arn)

	// Role Creation
	assert.Equal(t, "role", *mock.capturedCreateRoleInput.RoleName)
	assert.Equal(t, AssumePolicyDocument, *mock.capturedCreateRoleInput.AssumeRolePolicyDocument)

	// Role Policy Creation
	assert.Equal(t, "role", *mock.capturedGetRolePolicyInput.RoleName)
	assert.Equal(t, "corral-permissions", *mock.capturedGetRolePolicyInput.PolicyName)
	assert.Equal(t, AttachPolicyDocument, *mock.capturedPutRolePolicyInput.PolicyDocument)
}

func TestDeletePermissions(t *testing.T) {
	mock := &iamMock{}
	client := IAMClient{mock}

	err := client.DeletePermissions("testRole")
	assert.Nil(t, err)

	assert.Equal(t, "testRole", *mock.capturedDeleteRolePolicyInput.RoleName)
	assert.Equal(t, "corral-permissions", *mock.capturedDeleteRolePolicyInput.PolicyName)

	assert.Equal(t, "testRole", *mock.capturedDeleteRoleInput.RoleName)
}
