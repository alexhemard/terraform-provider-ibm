// Copyright IBM Corp. 2017, 2021 All Rights Reserved.
// Licensed under the Mozilla Public License v2.0

package ibm

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/url"
	"os"
	"strings"
	"time"

	rc "github.com/IBM/platform-services-go-sdk/resourcecontrollerv2"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/resource"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	validation "github.com/hashicorp/terraform-plugin-sdk/v2/helper/validation"

	//	"github.com/IBM-Cloud/bluemix-go/api/globaltagging/globaltaggingv3"
	"github.com/IBM-Cloud/bluemix-go/bmxerror"
	"github.com/IBM-Cloud/bluemix-go/models"

	"github.com/IBM/cloud-databases-go-sdk/clouddatabasesv5"
	"github.com/IBM/go-sdk-core/v5/core"
)

const (
	databaseInstanceSuccessStatus      = "active"
	databaseInstanceProvisioningStatus = "provisioning"
	databaseInstanceProgressStatus     = "in progress"
	databaseInstanceInactiveStatus     = "inactive"
	databaseInstanceFailStatus         = "failed"
	databaseInstanceRemovedStatus      = "removed"
	databaseInstanceReclamation        = "pending_reclamation"
)

const (
	databaseTaskSuccessStatus  = "completed"
	databaseTaskProgressStatus = "running"
	databaseTaskFailStatus     = "failed"
)

type ConnectionString struct {
	Name        string
	Password    string
	String      string
	Composed    []string
	Certificate struct {
		Name              string
		CertificateBase64 string
	}
	Hosts []struct {
		HostName string `json:"hostname"`
		Port     int    `json:"port"`
	}
	Scheme       string
	QueryOptions map[string]interface{}
	Path         string
	Database     interface{}
	BundleName   string
	BundleBase64 string
}

func resourceIBMDatabaseInstance() *schema.Resource {
	return &schema.Resource{
		Create:        resourceIBMDatabaseInstanceCreate,
		Read:          resourceIBMDatabaseInstanceRead,
		Update:        resourceIBMDatabaseInstanceUpdate,
		Delete:        resourceIBMDatabaseInstanceDelete,
		Exists:        resourceIBMDatabaseInstanceExists,
		CustomizeDiff: resourceIBMDatabaseInstanceDiff,
		Importer:      &schema.ResourceImporter{},

		Timeouts: &schema.ResourceTimeout{
			Create: schema.DefaultTimeout(60 * time.Minute),
			Update: schema.DefaultTimeout(60 * time.Minute),
			Delete: schema.DefaultTimeout(10 * time.Minute),
		},

		Schema: map[string]*schema.Schema{
			"name": {
				Description: "Resource instance name for example, my Database instance",
				Type:        schema.TypeString,
				Required:    true,
			},

			"resource_group_id": {
				Type:        schema.TypeString,
				Computed:    true,
				Optional:    true,
				ForceNew:    true,
				Description: "The id of the resource group in which the Database instance is present",
			},

			"location": {
				Description: "The location or the region in which Database instance exists",
				Type:        schema.TypeString,
				Required:    true,
			},

			"service": {
				Description:  "The name of the Cloud Internet database service",
				Type:         schema.TypeString,
				Required:     true,
				ValidateFunc: validateAllowedStringValue([]string{"databases-for-etcd", "databases-for-postgresql", "databases-for-redis", "databases-for-elasticsearch", "databases-for-mongodb", "messages-for-rabbitmq", "databases-for-mysql", "databases-for-cassandra", "databases-for-enterprisedb"}),
			},
			"plan": {
				Description:  "The plan type of the Database instance",
				Type:         schema.TypeString,
				Required:     true,
				ValidateFunc: validateAllowedStringValue([]string{"standard", "enterprise"}),
				ForceNew:     true,
			},

			"status": {
				Description: "The resource instance status",
				Type:        schema.TypeString,
				Computed:    true,
			},

			"guid": {
				Type:        schema.TypeString,
				Computed:    true,
				Description: "Unique identifier of resource instance",
			},

			"adminuser": {
				Description: "The admin user id for the instance",
				Type:        schema.TypeString,
				Computed:    true,
			},
			"adminpassword": {
				Description:  "The admin user password for the instance",
				Type:         schema.TypeString,
				Optional:     true,
				ValidateFunc: validation.StringLenBetween(10, 32),
				Sensitive:    true,
				// DiffSuppressFunc: func(k, old, new string, d *schema.ResourceData) bool {
				//  return true
				// },
			},
			"configuration": {
				Type:     schema.TypeString,
				Optional: true,
				StateFunc: func(v interface{}) string {
					json, err := normalizeJSONString(v)
					if err != nil {
						return fmt.Sprintf("%q", err.Error())
					}
					return json
				},
				Description: "The configuration in JSON format",
			},
			"version": {
				Description: "The database version to provision if specified",
				Type:        schema.TypeString,
				Optional:    true,
				Computed:    true,
				ForceNew:    true,
			},
			"members_memory_allocation_mb": {
				Description:   "Memory allocation required for cluster",
				Type:          schema.TypeInt,
				Optional:      true,
				Computed:      true,
				ConflictsWith: []string{"node_count", "node_memory_allocation_mb", "node_disk_allocation_mb", "node_cpu_allocation_count"},
			},
			"members_disk_allocation_mb": {
				Description:   "Disk allocation required for cluster",
				Type:          schema.TypeInt,
				Optional:      true,
				Computed:      true,
				ConflictsWith: []string{"node_count", "node_memory_allocation_mb", "node_disk_allocation_mb", "node_cpu_allocation_count"},
			},
			"members_cpu_allocation_count": {
				Description:   "CPU allocation required for cluster",
				Type:          schema.TypeInt,
				Optional:      true,
				Computed:      true,
				ConflictsWith: []string{"node_count", "node_memory_allocation_mb", "node_disk_allocation_mb", "node_cpu_allocation_count"},
			},
			"node_count": {
				Description:   "Total number of nodes in the cluster",
				Type:          schema.TypeInt,
				Optional:      true,
				Computed:      true,
				ConflictsWith: []string{"members_memory_allocation_mb", "members_disk_allocation_mb", "members_cpu_allocation_count"},
			},
			"node_memory_allocation_mb": {
				Description: "Memory allocation per node",
				Type:        schema.TypeInt,
				Optional:    true,
				Computed:    true,

				ConflictsWith: []string{"members_memory_allocation_mb", "members_disk_allocation_mb", "members_cpu_allocation_count"},
			},
			"node_disk_allocation_mb": {
				Description:   "Disk allocation per node",
				Type:          schema.TypeInt,
				Optional:      true,
				Computed:      true,
				ConflictsWith: []string{"members_memory_allocation_mb", "members_disk_allocation_mb", "members_cpu_allocation_count"},
			},
			"node_cpu_allocation_count": {
				Description:   "CPU allocation per node",
				Type:          schema.TypeInt,
				Optional:      true,
				Computed:      true,
				ConflictsWith: []string{"members_memory_allocation_mb", "members_disk_allocation_mb", "members_cpu_allocation_count"},
			},
			"plan_validation": {
				Description: "For elasticsearch and postgres perform database parameter validation during the plan phase. Otherwise, database parameter validation happens in apply phase.",
				Type:        schema.TypeBool,
				Optional:    true,
				Default:     true,
				DiffSuppressFunc: func(k, o, n string, d *schema.ResourceData) bool {
					if o == "" {
						return true
					}
					return false
				},
			},
			"service_endpoints": {
				Description:  "Types of the service endpoints. Possible values are 'public', 'private', 'public-and-private'.",
				Type:         schema.TypeString,
				Optional:     true,
				Default:      "public",
				ValidateFunc: validateAllowedStringValue([]string{"public", "private", "public-and-private"}),
			},
			"backup_id": {
				Description: "The CRN of backup source database",
				Type:        schema.TypeString,
				Optional:    true,
			},
			"remote_leader_id": {
				Description:      "The CRN of leader database",
				Type:             schema.TypeString,
				Optional:         true,
				DiffSuppressFunc: applyOnce,
			},
			"key_protect_instance": {
				Description: "The CRN of Key protect instance",
				Type:        schema.TypeString,
				Optional:    true,
				ForceNew:    true,
			},
			"key_protect_key": {
				Description: "The CRN of Key protect key",
				Type:        schema.TypeString,
				Optional:    true,
				ForceNew:    true,
			},
			"backup_encryption_key_crn": {
				Description: "The Backup Encryption Key CRN",
				Type:        schema.TypeString,
				Optional:    true,
				ForceNew:    true,
			},
			"tags": {
				Type:     schema.TypeSet,
				Optional: true,
				Computed: true,
				Elem:     &schema.Schema{Type: schema.TypeString, ValidateFunc: InvokeValidator("ibm_database", "tag")},
				Set:      resourceIBMVPCHash,
			},
			"point_in_time_recovery_deployment_id": {
				Description:      "The CRN of source instance",
				Type:             schema.TypeString,
				Optional:         true,
				DiffSuppressFunc: applyOnce,
			},
			"point_in_time_recovery_time": {
				Description:      "The point in time recovery time stamp of the deployed instance",
				Type:             schema.TypeString,
				Optional:         true,
				DiffSuppressFunc: applyOnce,
			},
			"users": {
				Type:     schema.TypeSet,
				Optional: true,
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"name": {
							Description:  "User name",
							Type:         schema.TypeString,
							Optional:     true,
							ValidateFunc: validation.StringLenBetween(5, 32),
						},
						"password": {
							Description:  "User password",
							Type:         schema.TypeString,
							Optional:     true,
							Sensitive:    true,
							ValidateFunc: validation.StringLenBetween(10, 32),
						},
						"user_type": {
							Description: "User type",
							Type:        schema.TypeString,
							Required:    true,
							Sensitive:   false,
						},
					},
				},
			},
			"connectionstrings": {
				Type:     schema.TypeList,
				Computed: true,
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"name": {
							Description: "User name",
							Type:        schema.TypeString,
							Computed:    true,
						},
						"composed": {
							Description: "Connection string",
							Type:        schema.TypeString,
							Computed:    true,
						},
						"scheme": {
							Description: "DB scheme",
							Type:        schema.TypeString,
							Computed:    true,
						},
						"certname": {
							Description: "Certificate Name",
							Type:        schema.TypeString,
							Computed:    true,
						},
						"certbase64": {
							Description: "Certificate in base64 encoding",
							Type:        schema.TypeString,
							Computed:    true,
						},
						"bundlename": {
							Description: "Cassandra Bundle Name",
							Type:        schema.TypeString,
							Computed:    true,
						},
						"bundlebase64": {
							Description: "Cassandra base64 encoding",
							Type:        schema.TypeString,
							Computed:    true,
						},
						"password": {
							Description: "Password",
							Type:        schema.TypeString,
							Computed:    true,
						},
						"queryoptions": {
							Description: "DB query options",
							Type:        schema.TypeString,
							Computed:    true,
						},
						"database": {
							Description: "DB name",
							Type:        schema.TypeString,
							Computed:    true,
						},
						"path": {
							Description: "DB path",
							Type:        schema.TypeString,
							Computed:    true,
						},
						"hosts": {
							Type:     schema.TypeList,
							Optional: true,
							Elem: &schema.Resource{
								Schema: map[string]*schema.Schema{
									"hostname": {
										Description: "DB host name",
										Type:        schema.TypeString,
										Computed:    true,
									},
									"port": {
										Description: "DB port",
										Type:        schema.TypeString,
										Computed:    true,
									},
								},
							},
						},
					},
				},
			},
			"whitelist": {
				Type:          schema.TypeSet,
				Optional:      true,
				Deprecated:    "use allowlist instead",
				ConflictsWith: []string{"allowlist"},
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"address": {
							Description:  "Whitelist IP address in CIDR notation",
							Type:         schema.TypeString,
							Optional:     true,
							ValidateFunc: validateCIDR,
						},
						"description": {
							Description:  "Unique white list description",
							Type:         schema.TypeString,
							Optional:     true,
							ValidateFunc: validation.StringLenBetween(1, 32),
						},
					},
				},
			},
			"allowlist": {
				Type:          schema.TypeSet,
				Optional:      true,
				ConflictsWith: []string{"whitelist"},
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"address": {
							Description:  "Allowlist IP address in CIDR notation",
							Type:         schema.TypeString,
							Optional:     true,
							ValidateFunc: validateCIDR,
						},
						"description": {
							Description:  "Unique allow list description",
							Type:         schema.TypeString,
							Optional:     true,
							ValidateFunc: validation.StringLenBetween(1, 32),
						},
					},
				},
			},
			"groups": {
				Type:     schema.TypeList,
				Computed: true,
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"group_id": {
							Description: "Scaling group name",
							Type:        schema.TypeString,
							Computed:    true,
						},
						"count": {
							Description: "Count of scaling groups for the instance",
							Type:        schema.TypeInt,
							Computed:    true,
						},
						"memory": {
							Type:     schema.TypeList,
							Computed: true,
							Elem: &schema.Resource{
								Schema: map[string]*schema.Schema{
									"units": {
										Type:        schema.TypeString,
										Computed:    true,
										Description: "The units memory is allocated in.",
									},
									"allocation_mb": {
										Type:        schema.TypeInt,
										Computed:    true,
										Description: "The current memory allocation for a group instance",
									},
									"minimum_mb": {
										Type:        schema.TypeInt,
										Computed:    true,
										Description: "The minimum memory size for a group instance",
									},
									"step_size_mb": {
										Type:        schema.TypeInt,
										Computed:    true,
										Description: "The step size memory increases or decreases in.",
									},
									"is_adjustable": {
										Type:        schema.TypeBool,
										Computed:    true,
										Description: "Is the memory size adjustable.",
									},
									"can_scale_down": {
										Type:        schema.TypeBool,
										Computed:    true,
										Description: "Can memory scale down as well as up.",
									},
								},
							},
						},
						"cpu": {
							Type:     schema.TypeList,
							Computed: true,
							Elem: &schema.Resource{
								Schema: map[string]*schema.Schema{
									"units": {
										Type:        schema.TypeString,
										Computed:    true,
										Description: "The .",
									},
									"allocation_count": {
										Type:        schema.TypeInt,
										Computed:    true,
										Description: "The current cpu allocation count",
									},
									"minimum_count": {
										Type:        schema.TypeInt,
										Computed:    true,
										Description: "The minimum number of cpus allowed",
									},
									"step_size_count": {
										Type:        schema.TypeInt,
										Computed:    true,
										Description: "The number of CPUs allowed to step up or down by",
									},
									"is_adjustable": {
										Type:        schema.TypeBool,
										Computed:    true,
										Description: "Are the number of CPUs adjustable",
									},
									"can_scale_down": {
										Type:        schema.TypeBool,
										Computed:    true,
										Description: "Can the number of CPUs be scaled down as well as up",
									},
								},
							},
						},
						"disk": {
							Type:     schema.TypeList,
							Computed: true,
							Elem: &schema.Resource{
								Schema: map[string]*schema.Schema{
									"units": {
										Type:        schema.TypeString,
										Computed:    true,
										Description: "The units disk is allocated in",
									},
									"allocation_mb": {
										Type:        schema.TypeInt,
										Computed:    true,
										Description: "The current disk allocation",
									},
									"minimum_mb": {
										Type:        schema.TypeInt,
										Computed:    true,
										Description: "The minimum disk size allowed",
									},
									"step_size_mb": {
										Type:        schema.TypeInt,
										Computed:    true,
										Description: "The step size disk increases or decreases in",
									},
									"is_adjustable": {
										Type:        schema.TypeBool,
										Computed:    true,
										Description: "Is the disk size adjustable",
									},
									"can_scale_down": {
										Type:        schema.TypeBool,
										Computed:    true,
										Description: "Can the disk size be scaled down as well as up",
									},
								},
							},
						},
					},
				},
			},
			"auto_scaling": {
				Type:        schema.TypeList,
				Description: "ICD Auto Scaling",
				Optional:    true,
				Computed:    true,
				MaxItems:    1,
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"disk": {
							Type:        schema.TypeList,
							Description: "Disk Auto Scaling",
							Optional:    true,
							Computed:    true,
							MaxItems:    1,
							Elem: &schema.Resource{
								Schema: map[string]*schema.Schema{
									"capacity_enabled": {
										Description: "Auto Scaling Scalar: Capacity Enabled",
										Type:        schema.TypeBool,
										Optional:    true,
										Computed:    true,
									},
									"free_space_less_than_percent": {
										Description: "Auto Scaling Scalar: Capacity Free Space Less Than Percent",
										Type:        schema.TypeInt,
										Optional:    true,
										Computed:    true,
									},
									"io_enabled": {
										Description: "Auto Scaling Scalar: IO Utilization Enabled",
										Type:        schema.TypeBool,
										Optional:    true,
										Computed:    true,
									},

									"io_over_period": {
										Description: "Auto Scaling Scalar: IO Utilization Over Period",
										Type:        schema.TypeString,
										Optional:    true,
										Computed:    true,
									},
									"io_above_percent": {
										Description: "Auto Scaling Scalar: IO Utilization Above Percent",
										Type:        schema.TypeInt,
										Optional:    true,
										Computed:    true,
									},
									"rate_increase_percent": {
										Description: "Auto Scaling Rate: Increase Percent",
										Type:        schema.TypeInt,
										Optional:    true,
										Computed:    true,
									},
									"rate_period_seconds": {
										Description: "Auto Scaling Rate: Period Seconds",
										Type:        schema.TypeInt,
										Optional:    true,
										Computed:    true,
									},
									"rate_limit_mb_per_member": {
										Description: "Auto Scaling Rate: Limit mb per member",
										Type:        schema.TypeInt,
										Optional:    true,
										Computed:    true,
									},
									"rate_units": {
										Description: "Auto Scaling Rate: Units ",
										Type:        schema.TypeString,
										Optional:    true,
										Computed:    true,
									},
								},
							},
						},
						"memory": {
							Type:        schema.TypeList,
							Description: "Memory Auto Scaling",
							Optional:    true,
							Computed:    true,
							MaxItems:    1,
							Elem: &schema.Resource{
								Schema: map[string]*schema.Schema{
									"io_enabled": {
										Description: "Auto Scaling Scalar: IO Utilization Enabled",
										Type:        schema.TypeBool,
										Optional:    true,
										Computed:    true,
									},

									"io_over_period": {
										Description: "Auto Scaling Scalar: IO Utilization Over Period",
										Type:        schema.TypeString,
										Optional:    true,
										Computed:    true,
									},
									"io_above_percent": {
										Description: "Auto Scaling Scalar: IO Utilization Above Percent",
										Type:        schema.TypeInt,
										Optional:    true,
										Computed:    true,
									},
									"rate_increase_percent": {
										Description: "Auto Scaling Rate: Increase Percent",
										Type:        schema.TypeInt,
										Optional:    true,
										Computed:    true,
									},
									"rate_period_seconds": {
										Description: "Auto Scaling Rate: Period Seconds",
										Type:        schema.TypeInt,
										Optional:    true,
										Computed:    true,
									},
									"rate_limit_mb_per_member": {
										Description: "Auto Scaling Rate: Limit mb per member",
										Type:        schema.TypeInt,
										Optional:    true,
										Computed:    true,
									},
									"rate_units": {
										Description: "Auto Scaling Rate: Units ",
										Type:        schema.TypeString,
										Optional:    true,
										Computed:    true,
									},
								},
							},
						},
						"cpu": {
							Type:        schema.TypeList,
							Description: "CPU Auto Scaling",
							Optional:    true,
							Computed:    true,
							MaxItems:    1,
							Elem: &schema.Resource{
								Schema: map[string]*schema.Schema{
									"rate_increase_percent": {
										Description: "Auto Scaling Rate: Increase Percent",
										Type:        schema.TypeInt,
										Optional:    true,
										Computed:    true,
									},
									"rate_period_seconds": {
										Description: "Auto Scaling Rate: Period Seconds",
										Type:        schema.TypeInt,
										Optional:    true,
										Computed:    true,
									},
									"rate_limit_count_per_member": {
										Description: "Auto Scaling Rate: Limit count per number",
										Type:        schema.TypeInt,
										Optional:    true,
										Computed:    true,
									},
									"rate_units": {
										Description: "Auto Scaling Rate: Units ",
										Type:        schema.TypeString,
										Optional:    true,
										Computed:    true,
									},
								},
							},
						},
					},
				},
			},
			ResourceName: {
				Type:        schema.TypeString,
				Computed:    true,
				Description: "The name of the resource",
			},

			ResourceCRN: {
				Type:        schema.TypeString,
				Computed:    true,
				Description: "The crn of the resource",
			},

			ResourceStatus: {
				Type:        schema.TypeString,
				Computed:    true,
				Description: "The status of the resource",
			},

			ResourceGroupName: {
				Type:        schema.TypeString,
				Computed:    true,
				Description: "The resource group name in which resource is provisioned",
			},
			ResourceControllerURL: {
				Type:        schema.TypeString,
				Computed:    true,
				Description: "The URL of the IBM Cloud dashboard that can be used to explore and view details about the resource",
			},
		},
	}
}
func resourceIBMICDValidator() *ResourceValidator {

	validateSchema := make([]ValidateSchema, 0)

	validateSchema = append(validateSchema,
		ValidateSchema{
			Identifier:                 "tag",
			ValidateFunctionIdentifier: ValidateRegexpLen,
			Type:                       TypeString,
			Optional:                   true,
			Regexp:                     `^[A-Za-z0-9:_ .-]+$`,
			MinValueLength:             1,
			MaxValueLength:             128})

	ibmICDResourceValidator := ResourceValidator{ResourceName: "ibm_database", Schema: validateSchema}
	return &ibmICDResourceValidator
}

type Params struct {
	Version             string `json:"version,omitempty"`
	KeyProtectKey       string `json:"disk_encryption_key_crn,omitempty"`
	BackUpEncryptionCRN string `json:"backup_encryption_key_crn,omitempty"`
	Memory              int    `json:"members_memory_allocation_mb,omitempty"`
	Disk                int    `json:"members_disk_allocation_mb,omitempty"`
	CPU                 int    `json:"members_cpu_allocation_count,omitempty"`
	KeyProtectInstance  string `json:"disk_encryption_instance_crn,omitempty"`
	ServiceEndpoints    string `json:"service-endpoints,omitempty"`
	BackupID            string `json:"backup-id,omitempty"`
	RemoteLeaderID      string `json:"remote_leader_id,omitempty"`
	PITRDeploymentID    string `json:"point_in_time_recovery_deployment_id,omitempty"`
	PITRTimeStamp       string `json:"point_in_time_recovery_time,omitempty"`
}

type GroupLimit struct {
	Units        string
	Allocation   int64
	Minimum      int64
	Maximum      int64
	StepSize     int64
	IsAdjustable bool
	IsOptional   bool
	CanScaleDown bool
}

type GroupMembers struct {
	clouddatabasesv5.GroupMembers
}

type GroupDisk struct {
	clouddatabasesv5.GroupDisk
}

type GroupMemory struct {
	clouddatabasesv5.GroupMemory
}

type GroupCPU struct {
	clouddatabasesv5.GroupCPU
}

func (g GroupMembers) GroupLimit() (groupLimit GroupLimit) {
	groupLimit = GroupLimit{
		Units:        *g.Units,
		Allocation:   *g.AllocationCount,
		Minimum:      *g.MinimumCount,
		Maximum:      *g.MaximumCount,
		StepSize:     *g.StepSizeCount,
		IsAdjustable: *g.IsAdjustable,
		CanScaleDown: *g.CanScaleDown,
	}
	return
}

func (g GroupDisk) GroupLimit() (groupLimit GroupLimit) {
	groupLimit = GroupLimit{
		Units:        *g.Units,
		Allocation:   *g.AllocationMb,
		Minimum:      *g.MinimumMb,
		Maximum:      *g.MaximumMb,
		StepSize:     *g.StepSizeMb,
		IsAdjustable: *g.IsAdjustable,
		CanScaleDown: *g.CanScaleDown,
	}
	return
}

func (g GroupMemory) GroupLimit() (groupLimit GroupLimit) {
	groupLimit = GroupLimit{
		Units:        *g.Units,
		Allocation:   *g.AllocationMb,
		Minimum:      *g.MinimumMb,
		Maximum:      *g.MaximumMb,
		StepSize:     *g.StepSizeMb,
		IsAdjustable: *g.IsAdjustable,
		CanScaleDown: *g.CanScaleDown,
	}
	return
}

func (g GroupCPU) GroupLimit() (groupLimit GroupLimit) {
	groupLimit = GroupLimit{
		Units:        *g.Units,
		Allocation:   *g.AllocationCount,
		Minimum:      *g.MinimumCount,
		Maximum:      *g.MaximumCount,
		StepSize:     *g.StepSizeCount,
		IsAdjustable: *g.IsAdjustable,
		CanScaleDown: *g.CanScaleDown,
	}
	return
}

func checkGroupValue(name string, limits GroupLimit, divider int64, diff *schema.ResourceDiff) error {
	if diff.HasChange(name) {
		oldSetting, newSetting := diff.GetChange(name)
		old := oldSetting.(int64)
		new := newSetting.(int64)

		if new < limits.Minimum/divider || new > limits.Maximum/divider || new%(limits.StepSize/divider) != 0 {
			return fmt.Errorf("%s must be >= %d and <= %d in increments of %d", name, limits.Minimum/divider, limits.Maximum/divider/divider, limits.StepSize/divider)
		}
		if old != new && !limits.IsAdjustable {
			return fmt.Errorf("%s can not change value after create", name)
		}
		if new < old && !limits.CanScaleDown {
			return fmt.Errorf("%s can not scale down from %d to %d", name, old, new)
		}
		return nil
	}
	return nil
}

func resourceIBMDatabaseInstanceDiff(_ context.Context, diff *schema.ResourceDiff, meta interface{}) error {
	err := resourceTagsCustomizeDiff(diff)
	if err != nil {
		return err
	}

	service := diff.Get("service").(string)
	if service == "databases-for-postgresql" || service == "databases-for-elasticsearch" || service == "databases-for-cassandra" || service == "databases-for-enterprisedb" {
		planPhase := diff.Get("plan_validation").(bool)

		if planPhase {

			groupDefaults, err := getDatabaseServiceDefaults(service, meta)
			if err != nil {
				return err
			}

			groupMemory := GroupMemory{*groupDefaults.Memory}.GroupLimit()
			groupMembers := GroupMembers{*groupDefaults.Members}.GroupLimit()
			groupDisk := GroupDisk{*groupDefaults.Disk}.GroupLimit()
			groupCPU := GroupCPU{*groupDefaults.CPU}.GroupLimit()

			err = checkGroupValue("members_memory_allocation_mb", groupMemory, 1, diff)
			if err != nil {
				return err
			}

			err = checkGroupValue("members_disk_allocation_mb", groupDisk, 1, diff)
			if err != nil {
				return err
			}

			err = checkGroupValue("members_cpu_allocation_count", groupCPU, 1, diff)
			if err != nil {
				return err
			}

			err = checkGroupValue("node_count", groupMembers, 1, diff)
			if err != nil {
				return err
			}

			divider := groupMembers.Minimum
			err = checkGroupValue("node_memory_allocation_mb", groupMemory, divider, diff)
			if err != nil {
				return err
			}

			err = checkGroupValue("node_disk_allocation_mb", groupDisk, divider, diff)
			if err != nil {
				return err
			}

			if diff.HasChange("node_cpu_allocation_count") {
				err = checkGroupValue("node_cpu_allocation_count", groupCPU, divider, diff)
				if err != nil {
					return err
				}
			} else if diff.HasChange("node_count") {
				if _, ok := diff.GetOk("node_cpu_allocation_count"); !ok {
					_, newSetting := diff.GetChange("node_count")
					min := groupCPU.Minimum / divider
					if newSetting != min {
						return fmt.Errorf("node_cpu_allocation_count must be set when node_count is greater then the minimum %d", min)
					}
				}
			}
		}
	} else if diff.HasChange("node_count") || diff.HasChange("node_memory_allocation_mb") || diff.HasChange("node_disk_allocation_mb") || diff.HasChange("node_cpu_allocation_count") {
		return fmt.Errorf("[ERROR] node_count, node_memory_allocation_mb, node_disk_allocation_mb, node_cpu_allocation_count only supported for postgresql, elasticsearch and cassandra")
	}

	return nil
}

// Replace with func wrapper for resourceIBMResourceInstanceCreate specifying serviceName := "database......."
func resourceIBMDatabaseInstanceCreate(d *schema.ResourceData, meta interface{}) error {
	rsConClient, err := meta.(ClientSession).ResourceControllerV2API()
	if err != nil {
		return err
	}

	location := d.Get("location").(string)
	name := d.Get("name").(string)
	plan := d.Get("plan").(string)
	serviceName := d.Get("service").(string)

	rsInst := rc.CreateResourceInstanceOptions{
		Name: &name,
	}

	rsCatClient, err := meta.(ClientSession).ResourceCatalogAPI()
	if err != nil {
		return err
	}
	rsCatRepo := rsCatClient.ResourceCatalog()

	serviceOff, err := rsCatRepo.FindByName(serviceName, true)
	if err != nil {
		return fmt.Errorf("[ERROR] Error retrieving database service offering: %s", err)
	}

	servicePlan, err := rsCatRepo.GetServicePlanID(serviceOff[0], plan)
	if err != nil {
		return fmt.Errorf("[ERROR] Error retrieving plan: %s", err)
	}
	rsInst.ResourcePlanID = &servicePlan

	deployments, err := rsCatRepo.ListDeployments(servicePlan)
	if err != nil {
		return fmt.Errorf("[ERROR] Error retrieving deployment for plan %s : %s", plan, err)
	}
	if len(deployments) == 0 {
		return fmt.Errorf("[ERROR] No deployment found for service plan : %s", plan)
	}
	deployments, supportedLocations := filterDatabaseDeployments(deployments, location)

	if len(deployments) == 0 {
		locationList := make([]string, 0, len(supportedLocations))
		for l := range supportedLocations {
			locationList = append(locationList, l)
		}
		return fmt.Errorf("[ERROR] No deployment found for service plan %s at location %s.\nValid location(s) are: %q", plan, location, locationList)
	}
	catalogCRN := deployments[0].CatalogCRN
	rsInst.Target = &catalogCRN

	if rsGrpID, ok := d.GetOk("resource_group_id"); ok {
		rgID := rsGrpID.(string)
		rsInst.ResourceGroup = &rgID
	} else {
		defaultRg, err := defaultResourceGroup(meta)
		if err != nil {
			return err
		}
		rsInst.ResourceGroup = &defaultRg
	}

	initialNodeCount, err := getInitialNodeCount(d, meta)
	if err != nil {
		return err
	}

	params := Params{}
	if memory, ok := d.GetOk("members_memory_allocation_mb"); ok {
		params.Memory = memory.(int)
	}
	if memory, ok := d.GetOk("node_memory_allocation_mb"); ok {
		params.Memory = memory.(int) * initialNodeCount
	}
	if disk, ok := d.GetOk("members_disk_allocation_mb"); ok {
		params.Disk = disk.(int)
	}
	if disk, ok := d.GetOk("node_disk_allocation_mb"); ok {
		params.Disk = disk.(int) * initialNodeCount
	}
	if cpu, ok := d.GetOk("members_cpu_allocation_count"); ok {
		params.CPU = cpu.(int)
	}
	if cpu, ok := d.GetOk("node_cpu_allocation_count"); ok {
		params.CPU = cpu.(int) * initialNodeCount
	}
	if version, ok := d.GetOk("version"); ok {
		params.Version = version.(string)
	}
	if keyProtect, ok := d.GetOk("key_protect_key"); ok {
		params.KeyProtectKey = keyProtect.(string)
	}
	if keyProtectInstance, ok := d.GetOk("key_protect_instance"); ok {
		params.KeyProtectInstance = keyProtectInstance.(string)
	}
	if backupID, ok := d.GetOk("backup_id"); ok {
		params.BackupID = backupID.(string)
	}
	if backUpEncryptionKey, ok := d.GetOk("backup_encryption_key_crn"); ok {
		params.BackUpEncryptionCRN = backUpEncryptionKey.(string)
	}
	if remoteLeader, ok := d.GetOk("remote_leader_id"); ok {
		params.RemoteLeaderID = remoteLeader.(string)
	}
	if pitrID, ok := d.GetOk("point_in_time_recovery_deployment_id"); ok {
		params.PITRDeploymentID = pitrID.(string)
	}
	if pitrTime, ok := d.GetOk("point_in_time_recovery_time"); ok {
		params.PITRTimeStamp = pitrTime.(string)
	}
	serviceEndpoint := d.Get("service_endpoints").(string)
	params.ServiceEndpoints = serviceEndpoint
	parameters, _ := json.Marshal(params)
	var raw map[string]interface{}
	json.Unmarshal(parameters, &raw)
	//paramString := string(parameters[:])
	rsInst.Parameters = raw

	instance, response, err := rsConClient.CreateResourceInstance(&rsInst)
	instanceId := *instance.ID

	if err != nil {
		return fmt.Errorf("[ERROR] Error creating database instance: %s %s", err, response)
	}
	d.SetId(*instance.ID)

	_, err = waitForDatabaseInstanceCreate(d, meta, *instance.ID)
	if err != nil {
		return fmt.Errorf(
			"[ERROR] Error waiting for create database instance (%s) to complete: %s", *instance.ID, err)
	}

	cloudDatabasesV5, err := meta.(ClientSession).CloudDatabasesAPI()
	if err != nil {
		return fmt.Errorf("[ERROR] Error getting database client settings: %s", err)
	}

	if node_count, ok := d.GetOk("node_count"); ok {
		if initialNodeCount != node_count {
			err = horizontalScale(d, meta, cloudDatabasesV5)
			if err != nil {
				return err
			}
		}
	}
	v := os.Getenv("IC_ENV_TAGS")
	if _, ok := d.GetOk("tags"); ok || v != "" {
		oldList, newList := d.GetChange("tags")
		err = UpdateTagsUsingCRN(oldList, newList, meta, *instance.CRN)
		if err != nil {
			log.Printf(
				"Error on create of ibm database (%s) tags: %s", d.Id(), err)
		}
	}

	if pw, ok := d.GetOk("adminpassword"); ok {
		adminPassword := pw.(string)
		getDeploymentInfoOptions := cloudDatabasesV5.NewGetDeploymentInfoOptions(
			instanceId,
		)

		deploymentInfoRepsonse, response, err := cloudDatabasesV5.GetDeploymentInfo(getDeploymentInfoOptions)

		if err != nil {
			if response.StatusCode == 404 {
				return fmt.Errorf("[ERROR] The database instance was not found in the region set for the Provider, or the default of us-south. Specify the correct region in the provider definition, or create a provider alias for the correct region. %v", err)
			}
			return fmt.Errorf("[ERROR] Error getting database config while updating adminpassword for: %s with error %s", instanceId, err)
		}

		deployment := deploymentInfoRepsonse.Deployment
		adminUsername := deployment.AdminUsernames["database"]

		passwordSettingUser := &clouddatabasesv5.APasswordSettingUser{
			Password: &adminPassword,
		}

		changeUserPasswordOptions := &clouddatabasesv5.ChangeUserPasswordOptions{
			ID:       &instanceId,
			UserType: core.StringPtr("database"),
			Username: &adminUsername,
			User:     passwordSettingUser,
		}

		changeUserPasswordResponse, _, err := cloudDatabasesV5.ChangeUserPassword(changeUserPasswordOptions)
		if err != nil {
			return fmt.Errorf(
				"[ERROR] Error waiting for update of database (%s) admin password task to complete: %s", instanceId, err)
		}

		taskId := *changeUserPasswordResponse.Task.ID
		_, err = waitForDatabaseTaskComplete(taskId, d, meta, d.Timeout(schema.TimeoutUpdate))
		if err != nil {
			return fmt.Errorf(
				"[ERROR] Error waiting for update of database (%s) admin password task to complete: %s", instanceId, err)
		}
	}

	if al, ok := d.GetOk("allowlist"); ok {
		allowlist := expandAllowlist(al.(*schema.Set))

		setAllowlistOptions := &clouddatabasesv5.SetAllowlistOptions{
			ID:          &instanceId,
			IPAddresses: allowlist,
		}

		setAllowlistResponse, _, err := cloudDatabasesV5.SetAllowlist(setAllowlistOptions)
		if err != nil {
			return fmt.Errorf("[ERROR] Error updating database allowlists: %s", err)
		}

		taskId := *setAllowlistResponse.Task.ID

		_, err = waitForDatabaseTaskComplete(taskId, d, meta, d.Timeout(schema.TimeoutCreate))
		if err != nil {
			return fmt.Errorf(
				"[ERROR] Error waiting for update of database (%s) allowlist task to complete: %s", instanceId, err)
		}
	}

	var (
		autoscalingDiskGroup   *clouddatabasesv5.AutoscalingDiskGroupDisk
		autoscalingMemoryGroup *clouddatabasesv5.AutoscalingMemoryGroupMemory
		autoscalingCPUGroup    *clouddatabasesv5.AutoscalingCPUGroupCPU
	)

	if cpuRecord, ok := d.GetOk("auto_scaling.0.cpu"); ok {
		cpuGroup, err := expandAutoscalingCPUGroup(d, cpuRecord)
		if err != nil {
			return fmt.Errorf("[ERROR] Error in getting cpuBody from expandAutoscalingCPUGroup %s", err)
		}
		autoscalingCPUGroup = &cpuGroup
	}

	if diskRecord, ok := d.GetOk("auto_scaling.0.disk"); ok {
		diskGroup, err := expandAutoscalingDiskGroup(d, diskRecord)
		if err != nil {
			return fmt.Errorf("[ERROR] Error in getting diskGroup from expandAutoscalingDiskGroup %s", err)
		}
		autoscalingDiskGroup = &diskGroup
	}

	if memoryRecord, ok := d.GetOk("auto_scaling.0.memory"); ok {
		memoryGroup, err := expandAutoscalingMemoryGroup(d, memoryRecord)
		if err != nil {
			return fmt.Errorf("[ERROR] Error in getting memoryBody from expandAutoscalingMemoryGroup %s", err)
		}

		autoscalingMemoryGroup = &memoryGroup
	}

	if autoscalingDiskGroup != nil ||
		autoscalingMemoryGroup != nil ||
		autoscalingCPUGroup != nil {
		autoscalingSetGroupAutoscaling := clouddatabasesv5.AutoscalingSetGroupAutoscaling{}

		if autoscalingDiskGroup != nil {
			autoscalingSetGroupAutoscaling.Disk = autoscalingDiskGroup
		}

		if autoscalingMemoryGroup != nil {
			autoscalingSetGroupAutoscaling.Memory = autoscalingMemoryGroup
		}

		if autoscalingCPUGroup != nil {
			autoscalingSetGroupAutoscaling.CPU = autoscalingCPUGroup
		}

		setAutoscalingConditionsOptions := &clouddatabasesv5.SetAutoscalingConditionsOptions{
			ID:          &instanceId,
			GroupID:     core.StringPtr("member"),
			Autoscaling: &autoscalingSetGroupAutoscaling,
		}

		setAutoscalingConditionsResponse, _, err := cloudDatabasesV5.SetAutoscalingConditions(setAutoscalingConditionsOptions)
		if err != nil {
			return fmt.Errorf("[ERROR] Error updating database auto_scaling: %s", err)
		}

		taskId := *setAutoscalingConditionsResponse.Task.ID

		_, err = waitForDatabaseTaskComplete(taskId, d, meta, d.Timeout(schema.TimeoutCreate))
		if err != nil {
			return fmt.Errorf(
				"[ERROR] Error waiting for database (%s) memory auto_scaling group update task to complete: %s", instanceId, err)
		}
	}

	if userList, ok := d.GetOk("users"); ok {
		for _, user := range userList.(*schema.Set).List() {
			userEl := user.(map[string]interface{})
			createDatabaseUserRequestUserModel := &clouddatabasesv5.CreateDatabaseUserRequestUser{
				Username: core.StringPtr(userEl["name"].(string)),
				Password: core.StringPtr(userEl["password"].(string)),
			}

			instanceId := d.Id()
			createDatabaseUserOptions := &clouddatabasesv5.CreateDatabaseUserOptions{
				ID:       &instanceId,
				UserType: core.StringPtr(userEl["user_type"].(string)),
				User:     createDatabaseUserRequestUserModel,
			}

			fmt.Printf("User: %v", createDatabaseUserOptions)
			createDatabaseUserResponse, response, err := cloudDatabasesV5.CreateDatabaseUser(createDatabaseUserOptions)

			if err != nil {
				return fmt.Errorf("[ERROR] Error creating database user (%s) entry: %s %v", userEl["name"], err, response)
			}

			taskId := *createDatabaseUserResponse.Task.ID

			_, err = waitForDatabaseTaskComplete(taskId, d, meta, d.Timeout(schema.TimeoutCreate))
			if err != nil {
				return fmt.Errorf(
					"[ERROR] Error waiting for update of database (%s) user (%s) create task to complete: %s", d.Id(), userEl["name"], err)
			}
		}
	}

	return resourceIBMDatabaseInstanceRead(d, meta)
}

func resourceIBMDatabaseInstanceRead(d *schema.ResourceData, meta interface{}) error {
	rsConClient, err := meta.(ClientSession).ResourceControllerV2API()
	if err != nil {
		return err
	}

	instanceId := d.Id()
	connectionEndpoint := "public"
	rsInst := rc.GetResourceInstanceOptions{
		ID: &instanceId,
	}
	instance, response, err := rsConClient.GetResourceInstance(&rsInst)
	if err != nil {
		if strings.Contains(err.Error(), "Object not found") ||
			strings.Contains(err.Error(), "status code: 404") {
			log.Printf("[WARN] Removing record from state because it's not found via the API")
			d.SetId("")
			return nil
		}
		return fmt.Errorf("[ERROR] Error retrieving resource instance: %s %s", err, response)
	}
	if strings.Contains(*instance.State, "removed") {
		log.Printf("[WARN] Removing instance from TF state because it's now in removed state")
		d.SetId("")
		return nil
	}

	tags, err := GetTagsUsingCRN(meta, *instance.CRN)
	if err != nil {
		log.Printf(
			"Error on get of ibm Database tags (%s) tags: %s", d.Id(), err)
	}
	d.Set("tags", tags)
	d.Set("name", *instance.Name)
	d.Set("status", *instance.State)
	d.Set("resource_group_id", *instance.ResourceGroupID)
	if instance.CRN != nil {
		location := strings.Split(*instance.CRN, ":")
		if len(location) > 5 {
			d.Set("location", location[5])
		}
	}
	d.Set("guid", *instance.GUID)

	if instance.Parameters != nil {
		if endpoint, ok := instance.Parameters["service-endpoints"]; ok {
			if endpoint == "private" {
				connectionEndpoint = "private"
			}
			d.Set("service_endpoints", endpoint)
		}
	}

	d.Set(ResourceName, *instance.Name)
	d.Set(ResourceCRN, *instance.CRN)
	d.Set(ResourceStatus, *instance.State)
	d.Set(ResourceGroupName, *instance.ResourceGroupCRN)

	rcontroller, err := getBaseController(meta)
	if err != nil {
		return err
	}
	d.Set(ResourceControllerURL, rcontroller+"/services/"+url.QueryEscape(*instance.CRN))

	rsCatClient, err := meta.(ClientSession).ResourceCatalogAPI()
	if err != nil {
		return err
	}
	rsCatRepo := rsCatClient.ResourceCatalog()

	serviceOff, err := rsCatRepo.GetServiceName(*instance.ResourceID)
	if err != nil {
		return fmt.Errorf("[ERROR] Error retrieving service offering: %s", err)
	}

	d.Set("service", serviceOff)

	servicePlan, err := rsCatRepo.GetServicePlanName(*instance.ResourcePlanID)
	if err != nil {
		return fmt.Errorf("[ERROR] Error retrieving plan: %s", err)
	}
	d.Set("plan", servicePlan)

	cloudDatabasesV5, err := meta.(ClientSession).CloudDatabasesAPI()
	if err != nil {
		return fmt.Errorf("[ERROR] Error getting database client settings: %s", err)
	}

	getDeploymentInfoOptions := cloudDatabasesV5.NewGetDeploymentInfoOptions(
		instanceId,
	)

	deploymentInfoRepsonse, response, err := cloudDatabasesV5.GetDeploymentInfo(getDeploymentInfoOptions)
	if err != nil {
		if response.StatusCode == 404 {
			return fmt.Errorf("[ERROR] The database instance was not found in the region set for the Provider. Specify the correct region in the provider definition. %v", err)
		}
		return fmt.Errorf("[ERROR] Error getting database config for: %s with error %s", instanceId, err)
	}

	deployment := deploymentInfoRepsonse.Deployment

	d.Set("adminuser", deployment.AdminUsernames["database"])
	d.Set("version", deployment.Version)

	listDeploymentScalingGroupsOptions := cloudDatabasesV5.NewListDeploymentScalingGroupsOptions(
		instanceId,
	)

	listDeploymentScalingGroupResponse, response, err := cloudDatabasesV5.ListDeploymentScalingGroups(listDeploymentScalingGroupsOptions)
	if err != nil {
		return fmt.Errorf("[ERROR] Error getting database groups: %s", err)
	}
	groups := listDeploymentScalingGroupResponse

	d.Set("groups", flattenIcdGroups(*groups))
	d.Set("node_count", groups.Groups[0].Members.AllocationCount)

	d.Set("members_memory_allocation_mb", groups.Groups[0].Memory.AllocationMb)
	d.Set("node_memory_allocation_mb", *groups.Groups[0].Memory.AllocationMb / *groups.Groups[0].Members.AllocationCount)

	d.Set("members_disk_allocation_mb", *groups.Groups[0].Disk.AllocationMb)
	d.Set("node_disk_allocation_mb", *groups.Groups[0].Disk.AllocationMb / *groups.Groups[0].Members.AllocationCount)

	d.Set("members_cpu_allocation_count", *groups.Groups[0].CPU.AllocationCount)
	d.Set("node_cpu_allocation_count", *groups.Groups[0].CPU.AllocationCount / *groups.Groups[0].Members.AllocationCount)

	getAutoscalingConditionsOptions := &clouddatabasesv5.GetAutoscalingConditionsOptions{
		ID:      &instanceId,
		GroupID: core.StringPtr("member"),
	}

	autoscalingGroup, _, err := cloudDatabasesV5.GetAutoscalingConditions(getAutoscalingConditionsOptions)
	if err != nil {
		return fmt.Errorf("[ERROR] Error getting database autoscaling groups: %s", err)
	}
	d.Set("auto_scaling", flattenICDAutoScalingGroup(*autoscalingGroup))

	getAllowlistOptions := &clouddatabasesv5.GetAllowlistOptions{
		ID: &instanceId,
	}

	allowlistResponse, _, err := cloudDatabasesV5.GetAllowlist(getAllowlistOptions)
	if err != nil {
		return fmt.Errorf("[ERROR] Error getting database allowlist: %s", err)
	}
	d.Set("allowlist", flattenAllowlist(*allowlistResponse))

	var connectionStrings []ConnectionString

	// ICD does not implement a GetUsers API. Users populated from tf configuration.
	tfusers := d.Get("users").(*schema.Set)
	users := expandUsers(tfusers)
	adminUsername := deployment.AdminUsernames["database"]
	user := clouddatabasesv5.CreateDatabaseUserRequestUser{
		Username: &adminUsername,
		UserType: core.StringPtr("database"),
	}
	users = append(users, user)

	for _, user := range users {
		userName := *user.Username
		userType := *user.UserType

		connection, err := getConnectionString(d, userName, userType, connectionEndpoint, meta)
		if err != nil {
			return fmt.Errorf("[ERROR] Error getting user connection string for user (%s): %s", userName, err)
		}
		connectionStrings = append(connectionStrings, connection)
	}

	d.Set("connectionstrings", flattenConnectionStrings(connectionStrings))

	return nil
}

func resourceIBMDatabaseInstanceUpdate(d *schema.ResourceData, meta interface{}) error {
	rsConClient, err := meta.(ClientSession).ResourceControllerV2API()
	if err != nil {
		return err
	}

	instanceId := d.Id()
	updateReq := rc.UpdateResourceInstanceOptions{
		ID: &instanceId,
	}
	update := false
	if d.HasChange("name") {
		name := d.Get("name").(string)
		updateReq.Name = &name
		update = true
	}
	if d.HasChange("service_endpoints") {
		params := Params{}
		params.ServiceEndpoints = d.Get("service_endpoints").(string)
		parameters, _ := json.Marshal(params)
		var raw map[string]interface{}
		json.Unmarshal(parameters, &raw)
		updateReq.Parameters = raw
		update = true
	}

	if update {
		_, response, err := rsConClient.UpdateResourceInstance(&updateReq)
		if err != nil {
			return fmt.Errorf("[ERROR] Error updating resource instance: %s %s", err, response)
		}

		_, err = waitForDatabaseInstanceUpdate(d, meta)
		if err != nil {
			return fmt.Errorf(
				"[ERROR] Error waiting for update of resource instance (%s) to complete: %s", d.Id(), err)
		}

	}

	if d.HasChange("tags") {

		oldList, newList := d.GetChange("tags")
		err = UpdateTagsUsingCRN(oldList, newList, meta, instanceId)
		if err != nil {
			log.Printf(
				"[ERROR] Error on update of Database (%s) tags: %s", d.Id(), err)
		}
	}

	cloudDatabasesV5, err := meta.(ClientSession).CloudDatabasesAPI()

	if err != nil {
		return fmt.Errorf("[ERROR] Error getting database client settings: %s", err)
	}

	if d.HasChange("node_count") {
		err = horizontalScale(d, meta, cloudDatabasesV5)
		if err != nil {
			return err
		}
	}

	if d.HasChange("configuration") {
		service := d.Get("service").(string)
		if service == "databases-for-postgresql" || service == "databases-for-redis" || service == "databases-for-enterprisedb" {
			if s, ok := d.GetOk("configuration"); ok {
				var (
					configuration    interface{}
					setConfiguration interface{}
				)

				json.Unmarshal([]byte(s.(string)), &configuration)
				err = clouddatabasesv5.UnmarshalSetConfigurationConfiguration(configuration.(map[string]json.RawMessage), &setConfiguration)

				if err != nil {
					return fmt.Errorf("[ERROR] Error parsing database (%s) configuration: %s", instanceId, err)
				}

				castSetConfiguration := setConfiguration.(clouddatabasesv5.SetConfigurationConfiguration)

				updateDatabaseConfigurationOptions := &clouddatabasesv5.UpdateDatabaseConfigurationOptions{
					ID:            &instanceId,
					Configuration: &castSetConfiguration,
				}

				updateDatabaseConfigurationResponse, _, err := cloudDatabasesV5.UpdateDatabaseConfiguration(updateDatabaseConfigurationOptions)

				if err != nil {
					return fmt.Errorf("[ERROR] Error updating database (%s) configuration: %s", instanceId, err)
				}

				taskId := *updateDatabaseConfigurationResponse.Task.ID

				_, err = waitForDatabaseTaskComplete(taskId, d, meta, d.Timeout(schema.TimeoutUpdate))

				if err != nil {
					return fmt.Errorf(
						"[ERROR] Error waiting for database (%s) configuration update task to complete: %s", instanceId, err)
				}
			}

		} else {
			return fmt.Errorf("[ERROR] given database type %s is not configurable", service)
		}
	}

	if d.HasChange("members_memory_allocation_mb") ||
		d.HasChange("members_disk_allocation_mb") ||
		d.HasChange("members_cpu_allocation_count") ||
		d.HasChange("node_memory_allocation_mb") ||
		d.HasChange("node_disk_allocation_mb") ||
		d.HasChange("node_cpu_allocation_count") {

		var (
			setGroupMemory *clouddatabasesv5.SetMemoryGroupMemory
			setGroupDisk   *clouddatabasesv5.SetDiskGroupDisk
			setGroupCPU    *clouddatabasesv5.SetCPUGroupCPU
		)

		setDeploymentScalingGroupRequest := clouddatabasesv5.SetDeploymentScalingGroupRequest{}

		if d.HasChange("members_memory_allocation_mb") {
			memory := d.Get("members_memory_allocation_mb").(int64)
			memoryReq := clouddatabasesv5.SetMemoryGroupMemory{AllocationMb: &memory}
			setGroupMemory = &memoryReq
		}

		if d.HasChange("node_memory_allocation_mb") || d.HasChange("node_count") {
			memory := d.Get("node_memory_allocation_mb").(int64)
			count := d.Get("node_count").(int64)
			groupMemory := clouddatabasesv5.SetMemoryGroupMemory{AllocationMb: core.Int64Ptr(memory * count)}
			setGroupMemory = &groupMemory
		}

		if d.HasChange("members_disk_allocation_mb") {
			disk := d.Get("members_disk_allocation_mb").(int64)
			groupDisk := clouddatabasesv5.SetDiskGroupDisk{AllocationMb: &disk}
			setGroupDisk = &groupDisk
		}

		if d.HasChange("node_disk_allocation_mb") || d.HasChange("node_count") {
			disk := d.Get("node_disk_allocation_mb").(int64)
			count := d.Get("node_count").(int64)
			groupDisk := clouddatabasesv5.SetDiskGroupDisk{AllocationMb: core.Int64Ptr(disk * count)}
			setGroupDisk = &groupDisk
		}

		if d.HasChange("members_cpu_allocation_count") {
			cpu := d.Get("members_cpu_allocation_count").(int64)
			groupCPU := clouddatabasesv5.SetCPUGroupCPU{AllocationCount: &cpu}
			setGroupCPU = &groupCPU
		}

		if d.HasChange("node_cpu_allocation_mb") || d.HasChange("node_count") {
			cpu := d.Get("node_cpu_allocation_count").(int64)
			count := d.Get("node_count").(int64)
			groupCPU := clouddatabasesv5.SetCPUGroupCPU{AllocationCount: core.Int64Ptr(cpu * count)}
			setGroupCPU = &groupCPU
		}

		if setGroupDisk != nil {
			setDeploymentScalingGroupRequest.Disk = setGroupDisk
		}

		if setGroupMemory != nil {
			setDeploymentScalingGroupRequest.Memory = setGroupMemory
		}

		if setGroupCPU != nil {
			setDeploymentScalingGroupRequest.CPU = setGroupCPU
		}

		setDeploymentScalingGroupOptions := &clouddatabasesv5.SetDeploymentScalingGroupOptions{
			ID:                               &instanceId,
			GroupID:                          core.StringPtr("member"),
			SetDeploymentScalingGroupRequest: &setDeploymentScalingGroupRequest,
		}

		setDeploymentScalingGroupResponse, _, err := cloudDatabasesV5.SetDeploymentScalingGroup(setDeploymentScalingGroupOptions)
		if err != nil {
			return fmt.Errorf("[ERROR] Error updating database scaling group: %s", err)
		}

		taskId := *setDeploymentScalingGroupResponse.Task.ID

		_, err = waitForDatabaseTaskComplete(taskId, d, meta, d.Timeout(schema.TimeoutUpdate))
		if err != nil {
			return fmt.Errorf(
				"[ERROR] Error waiting for database (%s) scaling group update task to complete: %s", instanceId, err)
		}
	}

	if d.HasChange("auto_scaling.0.cpu") {
		cpuRecord := d.Get("auto_scaling.0.cpu")

		cpuBody, err := expandAutoscalingCPUGroup(d, cpuRecord)
		if err != nil {
			return fmt.Errorf("[ERROR] Error in getting cpuBody from expandAutoscalingCPUGroup %s", err)
		}

		autoscalingSetGroupAutoscaling := &clouddatabasesv5.AutoscalingSetGroupAutoscalingAutoscalingCPUGroup{
			CPU: &cpuBody,
		}

		setAutoscalingConditionsOptions := &clouddatabasesv5.SetAutoscalingConditionsOptions{
			ID:          &instanceId,
			GroupID:     core.StringPtr("member"),
			Autoscaling: autoscalingSetGroupAutoscaling,
		}

		setAutoscalingConditionsResponse, _, err := cloudDatabasesV5.SetAutoscalingConditions(setAutoscalingConditionsOptions)
		if err != nil {
			return fmt.Errorf("[ERROR] Error updating database cpu auto_scaling group: %s", err)
		}

		taskId := *setAutoscalingConditionsResponse.Task.ID

		_, err = waitForDatabaseTaskComplete(taskId, d, meta, d.Timeout(schema.TimeoutUpdate))
		if err != nil {
			return fmt.Errorf(
				"[ERROR] Error waiting for database (%s) cpu auto_scaling group update task to complete: %s", instanceId, err)
		}
	}
	if d.HasChange("auto_scaling.0.disk") {
		diskRecord := d.Get("auto_scaling.0.disk")

		diskBody, err := expandAutoscalingDiskGroup(d, diskRecord)
		if err != nil {
			return fmt.Errorf("[ERROR] Error in getting diskBody from expandAutoscalingDiskGroup %s", err)
		}

		autoscalingSetGroupAutoscaling := &clouddatabasesv5.AutoscalingSetGroupAutoscalingAutoscalingDiskGroup{
			Disk: &diskBody,
		}

		setAutoscalingConditionsOptions := &clouddatabasesv5.SetAutoscalingConditionsOptions{
			ID:          &instanceId,
			GroupID:     core.StringPtr("member"),
			Autoscaling: autoscalingSetGroupAutoscaling,
		}

		setAutoscalingConditionsResponse, _, err := cloudDatabasesV5.SetAutoscalingConditions(setAutoscalingConditionsOptions)
		if err != nil {
			return fmt.Errorf("[ERROR] Error updating database disk auto_scaling group: %s", err)
		}

		taskId := *setAutoscalingConditionsResponse.Task.ID

		_, err = waitForDatabaseTaskComplete(taskId, d, meta, d.Timeout(schema.TimeoutUpdate))
		if err != nil {
			return fmt.Errorf(
				"[ERROR] Error waiting for database (%s) cpu auto_scaling group update task to complete: %s", instanceId, err)
		}

	}

	if d.HasChange("auto_scaling.0.memory") {
		memoryRecord := d.Get("auto_scaling.0.memory")
		memoryBody, err := expandAutoscalingMemoryGroup(d, memoryRecord)
		if err != nil {
			return fmt.Errorf("[ERROR] Error in getting memoryBody from expandAutoscalingMemoryGroup %s", err)
		}

		autoscalingSetGroupAutoscaling := &clouddatabasesv5.AutoscalingSetGroupAutoscalingAutoscalingMemoryGroup{
			Memory: &memoryBody,
		}

		setAutoscalingConditionsOptions := &clouddatabasesv5.SetAutoscalingConditionsOptions{
			ID:          &instanceId,
			GroupID:     core.StringPtr("member"),
			Autoscaling: autoscalingSetGroupAutoscaling,
		}

		setAutoscalingConditionsResponse, _, err := cloudDatabasesV5.SetAutoscalingConditions(setAutoscalingConditionsOptions)
		if err != nil {
			return fmt.Errorf("[ERROR] Error updating database memory auto_scaling group: %s", err)
		}

		taskId := *setAutoscalingConditionsResponse.Task.ID

		_, err = waitForDatabaseTaskComplete(taskId, d, meta, d.Timeout(schema.TimeoutUpdate))
		if err != nil {
			return fmt.Errorf(
				"[ERROR] Error waiting for database (%s) cpu auto_scaling group update task to complete: %s", instanceId, err)
		}
	}

	if d.HasChange("adminpassword") {
		adminUsername := d.Get("adminuser").(string)
		adminPassword := d.Get("adminpassword").(string)

		passwordSettingUser := &clouddatabasesv5.APasswordSettingUser{
			Password: &adminPassword,
		}

		changeUserPasswordOptions := &clouddatabasesv5.ChangeUserPasswordOptions{
			ID:       &instanceId,
			UserType: core.StringPtr("database"),
			Username: &adminUsername,
			User:     passwordSettingUser,
		}

		changeUserPasswordResponse, _, err := cloudDatabasesV5.ChangeUserPassword(changeUserPasswordOptions)
		if err != nil {
			return fmt.Errorf(
				"[ERROR] Error waiting for update of database (%s) admin password task to complete: %s", instanceId, err)
		}

		taskId := *changeUserPasswordResponse.Task.ID
		_, err = waitForDatabaseTaskComplete(taskId, d, meta, d.Timeout(schema.TimeoutUpdate))
		if err != nil {
			return fmt.Errorf(
				"[ERROR] Error waiting for update of database (%s) admin password task to complete: %s", instanceId, err)
		}
	}

	if d.HasChange("allowlist") {
		_, newList := d.GetChange("allowlist")

		if newList == nil {
			newList = new(schema.Set)
		}

		ns := newList.(*schema.Set)

		allowlist := expandAllowlist(ns)
		setAllowlistOptions := &clouddatabasesv5.SetAllowlistOptions{
			ID:          &instanceId,
			IPAddresses: allowlist,
		}

		setAllowlistResponse, _, err := cloudDatabasesV5.SetAllowlist(setAllowlistOptions)
		if err != nil {
			return fmt.Errorf("[ERROR] Error updating database allowlist entry: %s", err)
		}

		taskId := *setAllowlistResponse.Task.ID

		_, err = waitForDatabaseTaskComplete(taskId, d, meta, d.Timeout(schema.TimeoutCreate))
		if err != nil {
			return fmt.Errorf(
				"[ERROR] Error waiting for update of database (%s) whitelist task to complete: %s", instanceId, err)
		}
	}

	if d.HasChange("users") {
		oldList, newList := d.GetChange("users")
		if oldList == nil {
			oldList = new(schema.Set)
		}
		if newList == nil {
			newList = new(schema.Set)
		}
		os := oldList.(*schema.Set)
		ns := newList.(*schema.Set)
		remove := os.Difference(ns).List()
		add := ns.Difference(os).List()

		if len(add) > 0 {
			for _, entry := range add {
				newEntry := entry.(map[string]interface{})
				userEntry := &clouddatabasesv5.CreateDatabaseUserRequestUser{
					Username: core.StringPtr(newEntry["name"].(string)),
					Password: core.StringPtr(newEntry["password"].(string)),
				}

				createDatabaseUserOptions := &clouddatabasesv5.CreateDatabaseUserOptions{
					ID:       &instanceId,
					UserType: core.StringPtr(newEntry["user_type"].(string)),
					User:     userEntry,
				}

				createDatabaseUserResponse, _, err := cloudDatabasesV5.CreateDatabaseUser(createDatabaseUserOptions)

				if err != nil {
					// ICD does not report if error was due to user already being defined. Check if can
					// successfully update password by itself.
					passwordSettingUser := &clouddatabasesv5.APasswordSettingUser{
						Password: core.StringPtr(newEntry["password"].(string)),
					}

					changeUserPasswordOptions := &clouddatabasesv5.ChangeUserPasswordOptions{
						ID:       &instanceId,
						UserType: core.StringPtr(newEntry["user_type"].(string)),
						Username: core.StringPtr(newEntry["name"].(string)),
						User:     passwordSettingUser,
					}

					changeUserPasswordResponse, _, err := cloudDatabasesV5.ChangeUserPassword(changeUserPasswordOptions)
					if err != nil {
						return fmt.Errorf("[ERROR] Error updating database user (%s) password: %s", newEntry["name"].(string), err)
					}

					taskId := *changeUserPasswordResponse.Task.ID
					_, err = waitForDatabaseTaskComplete(taskId, d, meta, d.Timeout(schema.TimeoutUpdate))
					if err != nil {
						return fmt.Errorf(
							"[ERROR] Error waiting for database (%s) user (%s) password update task to complete: %s", instanceId, newEntry["name"].(string), err)
					}
				} else {
					taskId := *createDatabaseUserResponse.Task.ID
					_, err = waitForDatabaseTaskComplete(taskId, d, meta, d.Timeout(schema.TimeoutUpdate))
					if err != nil {
						return fmt.Errorf(
							"[ERROR] Error waiting for database (%s) user (%s) create task to complete: %s", instanceId, newEntry["name"].(string), err)
					}
				}
			}
		}

		if len(remove) > 0 {
			for _, entry := range remove {
				newEntry := entry.(map[string]interface{})
				deleteDatabaseUserOptions := &clouddatabasesv5.DeleteDatabaseUserOptions{
					ID:       &instanceId,
					UserType: core.StringPtr(newEntry["user_type"].(string)),
					Username: core.StringPtr(newEntry["name"].(string)),
				}

				deleteDatabaseUserResponse, _, err := cloudDatabasesV5.DeleteDatabaseUser(deleteDatabaseUserOptions)
				if err != nil {
					return fmt.Errorf("[ERROR] Error deleting database user (%s) entry: %s", *deleteDatabaseUserOptions.Username, err)
				}
				taskId := *deleteDatabaseUserResponse.Task.ID
				_, err = waitForDatabaseTaskComplete(taskId, d, meta, d.Timeout(schema.TimeoutUpdate))
				if err != nil {
					return fmt.Errorf(
						"[ERROR] Error waiting for database (%s) user (%s) delete task to complete: %s", instanceId, *deleteDatabaseUserOptions.Username, err)
				}
			}
		}
	}

	return resourceIBMDatabaseInstanceRead(d, meta)
}

func horizontalScale(d *schema.ResourceData, meta interface{}, cloudDatabasesV5 clouddatabasesv5.CloudDatabasesV5) error {
	instanceId := d.Id()
	members := d.Get("node_count").(int64)

	setMembersGroupMembersModel := &clouddatabasesv5.SetMembersGroupMembers{
		AllocationCount: &members,
	}

	setDeploymentScalingGroupRequest := &clouddatabasesv5.SetDeploymentScalingGroupRequest{
		Members: setMembersGroupMembersModel,
	}

	setDeploymentScalingGroupOptions := &clouddatabasesv5.SetDeploymentScalingGroupOptions{
		ID:                               &instanceId,
		GroupID:                          core.StringPtr("members"),
		SetDeploymentScalingGroupRequest: setDeploymentScalingGroupRequest,
	}

	setDeploymentScalingGroupResponse, response, err := cloudDatabasesV5.SetDeploymentScalingGroup(setDeploymentScalingGroupOptions)

	if err != nil {
		return fmt.Errorf("\nError horizontally scaling: %s", response.Result.(map[string]interface{}))
	}

	taskId := *setDeploymentScalingGroupResponse.Task.ID

	_, err = waitForDatabaseTaskComplete(taskId, d, meta, d.Timeout(schema.TimeoutUpdate))
	if err != nil {
		return fmt.Errorf(
			"[ERROR] Error waiting for database (%s) horizontal scale task to complete: %s", instanceId, err)
	}

	return nil
}

func resourceIBMDatabaseInstanceDelete(d *schema.ResourceData, meta interface{}) error {
	rsConClient, err := meta.(ClientSession).ResourceControllerV2API()
	if err != nil {
		return err
	}
	id := d.Id()
	recursive := true
	deleteReq := rc.DeleteResourceInstanceOptions{
		Recursive: &recursive,
		ID:        &id,
	}
	response, err := rsConClient.DeleteResourceInstance(&deleteReq)
	if err != nil {
		// If prior delete occurs, instance is not immediately deleted, but remains in "removed" state"
		// RC 410 with "Gone" returned as error
		if strings.Contains(err.Error(), "Gone") ||
			strings.Contains(err.Error(), "status code: 410") {
			log.Printf("[WARN] Resource instance already deleted %s\n ", err)
			err = nil
		} else {
			return fmt.Errorf("[ERROR] Error deleting resource instance: %s %s ", err, response)
		}
	}

	_, err = waitForDatabaseInstanceDelete(d, meta)
	if err != nil {
		return fmt.Errorf(
			"[ERROR] Error waiting for resource instance (%s) to be deleted: %s", d.Id(), err)
	}

	d.SetId("")

	return nil
}
func resourceIBMDatabaseInstanceExists(d *schema.ResourceData, meta interface{}) (bool, error) {
	rsConClient, err := meta.(ClientSession).ResourceControllerV2API()
	if err != nil {
		return false, err
	}
	instanceId := d.Id()
	rsInst := rc.GetResourceInstanceOptions{
		ID: &instanceId,
	}
	instance, response, err := rsConClient.GetResourceInstance(&rsInst)
	if err != nil {
		if apiErr, ok := err.(bmxerror.RequestFailure); ok {
			if apiErr.StatusCode() == 404 {
				return false, nil
			}
		}
		return false, fmt.Errorf("[ERROR] Error getting database: %s %s", err, response)
	}
	if instance != nil && (strings.Contains(*instance.State, "removed") || strings.Contains(*instance.State, databaseInstanceReclamation)) {
		log.Printf("[WARN] Removing instance from state because it's in removed or pending_reclamation state")
		d.SetId("")
		return false, nil
	}

	return *instance.ID == instanceId, nil
}

func waitForDeploymentReady(meta interface{}, instanceId string) error {
	cloudDatabasesV5, err := meta.(ClientSession).CloudDatabasesAPI()

	if err != nil {
		return fmt.Errorf("[ERROR] Error getting database client settings: %s", err)
	}

	getDeploymentInfoOptions := cloudDatabasesV5.NewGetDeploymentInfoOptions(
		instanceId,
	)

	// Wait for ICD Interface
	err = retry(func() (err error) {
		_, response, err := cloudDatabasesV5.GetDeploymentInfo(getDeploymentInfoOptions)

		if err != nil {
			if response.StatusCode == 404 {
				return fmt.Errorf("The database instance was not found in the region set for the Provider, or the default of us-south. Specify the correct region in the provider definition, or create a provider alias for the correct region. %v", err)
			}
			return fmt.Errorf("Error getting database config for: %s with error %s\n", instanceId, err)
		}
		return nil
	})
	if err != nil {
		return err
	}
	return nil
}

func waitForDatabaseInstanceCreate(d *schema.ResourceData, meta interface{}, instanceId string) (interface{}, error) {
	rsConClient, err := meta.(ClientSession).ResourceControllerV2API()
	if err != nil {
		return false, err
	}

	stateConf := &resource.StateChangeConf{
		Pending: []string{databaseInstanceProgressStatus, databaseInstanceInactiveStatus, databaseInstanceProvisioningStatus},
		Target:  []string{databaseInstanceSuccessStatus},
		Refresh: func() (interface{}, string, error) {
			rsInst := rc.GetResourceInstanceOptions{
				ID: &instanceId,
			}
			instance, response, err := rsConClient.GetResourceInstance(&rsInst)
			if err != nil || instance == nil {
				if apiErr, ok := err.(bmxerror.RequestFailure); ok && apiErr.StatusCode() == 404 {
					return nil, "", fmt.Errorf("[ERROR] The resource instance %s does not exist anymore: %s %s", d.Id(), err, response)
				}
				return nil, "", fmt.Errorf("[ERROR] GetResourceInstance on %s failed with error %s %s", d.Id(), err, response)
			}
			if *instance.State == databaseInstanceFailStatus {
				return *instance, *instance.State, fmt.Errorf("[ERROR] The resource instance %s failed: %s %s", d.Id(), err, response)
			}
			return *instance, *instance.State, nil
		},
		Timeout:    d.Timeout(schema.TimeoutCreate),
		Delay:      10 * time.Second,
		MinTimeout: 10 * time.Second,
	}

	waitErr := waitForDeploymentReady(meta, instanceId)
	if waitErr != nil {
		return false, fmt.Errorf("Error ICD interface not ready after create: %s with error %s\n", instanceId, waitErr)

	}

	return stateConf.WaitForState()
}

func waitForDatabaseInstanceUpdate(d *schema.ResourceData, meta interface{}) (interface{}, error) {
	rsConClient, err := meta.(ClientSession).ResourceControllerV2API()
	if err != nil {
		return false, err
	}
	instanceId := d.Id()

	stateConf := &resource.StateChangeConf{
		Pending: []string{databaseInstanceProgressStatus, databaseInstanceInactiveStatus},
		Target:  []string{databaseInstanceSuccessStatus},
		Refresh: func() (interface{}, string, error) {
			rsInst := rc.GetResourceInstanceOptions{
				ID: &instanceId,
			}
			instance, response, err := rsConClient.GetResourceInstance(&rsInst)
			if err != nil {
				if apiErr, ok := err.(bmxerror.RequestFailure); ok && apiErr.StatusCode() == 404 {
					return nil, "", fmt.Errorf("[ERROR] The resource instance %s does not exist anymore: %s %s", d.Id(), err, response)
				}
				return nil, "", fmt.Errorf("[ERROR] GetResourceInstance on %s failed with error %s %s", d.Id(), err, response)
			}
			if *instance.State == databaseInstanceFailStatus {
				return *instance, *instance.State, fmt.Errorf("[ERROR] The resource instance %s failed: %s %s", d.Id(), err, response)
			}
			return *instance, *instance.State, nil
		},
		Timeout:    d.Timeout(schema.TimeoutUpdate),
		Delay:      10 * time.Second,
		MinTimeout: 10 * time.Second,
	}

	waitErr := waitForDeploymentReady(meta, instanceId)
	if waitErr != nil {
		return false, fmt.Errorf("Error ICD interface not ready after update: %s with error %s\n", instanceId, waitErr)

	}

	return stateConf.WaitForState()
}

func waitForDatabaseTaskComplete(taskId string, d *schema.ResourceData, meta interface{}, t time.Duration) (bool, error) {
	cloudDatabasesV5, err := meta.(ClientSession).CloudDatabasesAPI()

	if err != nil {
		return false, fmt.Errorf("[ERROR] Error getting database client settings: %s", err)
	}

	delayDuration := 5 * time.Second

	timeout := time.After(t)
	delay := time.Tick(delayDuration)
	getTaskOptions := &clouddatabasesv5.GetTaskOptions{
		ID: &taskId,
	}

	for {
		select {
		case <-timeout:
			return false, fmt.Errorf("[Error] Time out waiting for database task to complete")
		case <-delay:
			getTaskResponse, _, err := cloudDatabasesV5.GetTask(getTaskOptions)

			if err != nil {
				return false, fmt.Errorf("[ERROR] Database Task errored: %v", err)
			}

			switch *getTaskResponse.Task.Status {
			case "failed":
				return false, fmt.Errorf("[Error] Database Task failed")
			case "complete", "":
				return true, nil
			case "queued", "running":
				break
			}
		}
	}

}

func waitForDatabaseInstanceDelete(d *schema.ResourceData, meta interface{}) (interface{}, error) {
	rsConClient, err := meta.(ClientSession).ResourceControllerV2API()
	if err != nil {
		return false, err
	}
	instanceId := d.Id()
	stateConf := &resource.StateChangeConf{
		Pending: []string{databaseInstanceProgressStatus, databaseInstanceInactiveStatus, databaseInstanceSuccessStatus},
		Target:  []string{databaseInstanceRemovedStatus, databaseInstanceReclamation},
		Refresh: func() (interface{}, string, error) {
			rsInst := rc.GetResourceInstanceOptions{
				ID: &instanceId,
			}
			instance, response, err := rsConClient.GetResourceInstance(&rsInst)
			if err != nil {
				if apiErr, ok := err.(bmxerror.RequestFailure); ok && apiErr.StatusCode() == 404 {
					return instance, databaseInstanceSuccessStatus, nil
				}
				return nil, "", fmt.Errorf("[ERROR] GetResourceInstance on %s failed with error %s %s", d.Id(), err, response)
			}
			if *instance.State == databaseInstanceFailStatus {
				return instance, *instance.State, fmt.Errorf("[ERROR] The resource instance %s failed to delete: %s %s", d.Id(), err, response)
			}
			return *instance, *instance.State, nil
		},
		Timeout:    d.Timeout(schema.TimeoutDelete),
		Delay:      10 * time.Second,
		MinTimeout: 10 * time.Second,
	}

	return stateConf.WaitForState()
}

func filterDatabaseDeployments(deployments []models.ServiceDeployment, location string) ([]models.ServiceDeployment, map[string]bool) {
	supportedDeployments := []models.ServiceDeployment{}
	supportedLocations := make(map[string]bool)
	for _, d := range deployments {
		if d.Metadata.RCCompatible {
			deploymentLocation := d.Metadata.Deployment.Location
			supportedLocations[deploymentLocation] = true
			if deploymentLocation == location {
				supportedDeployments = append(supportedDeployments, d)
			}
		}
	}
	return supportedDeployments, supportedLocations
}

func getConnectionString(d *schema.ResourceData, userName, userType, endpointType string, meta interface{}) (ConnectionString, error) {
	cloudDatabasesV5, err := meta.(ClientSession).CloudDatabasesAPI()

	if err != nil {
		return ConnectionString{}, fmt.Errorf("[ERROR] Error getting database client settings: %s", err)
	}

	var connectionString *ConnectionString

	instanceId := d.Id()

	getConnectionOptions := &clouddatabasesv5.GetConnectionOptions{
		ID:           &instanceId,
		UserType:     &userType,
		UserID:       &userName,
		EndpointType: &endpointType,
	}

	connectionResponse, _, err := cloudDatabasesV5.GetConnection(getConnectionOptions)
	if err != nil {
		return ConnectionString{}, fmt.Errorf("[ERROR] Error getting database user connection string via ICD API: %s", err)
	}

	service := d.Get("service")

	var cdbConnection interface{}

	connection := connectionResponse.Connection.(*clouddatabasesv5.ConnectionConnection)
	switch service {
	case "databases-for-postgresql":
		cdbConnection = connection.Postgres
	case "databases-for-redis":
		cdbConnection = connection.Rediss
	case "databases-for-mongodb":
		cdbConnection = connection.Mongodb
	case "databases-for-elasticsearch":
		cdbConnection = connection.HTTPS
	case "databases-for-etcd":
		cdbConnection = connection.Grpc
	case "messages-for-rabbitmq":
		cdbConnection = connection.Amqps
	case "databases-for-enterprisedb":
		cdbConnection = connection.Postgres
	default:
		return ConnectionString{}, fmt.Errorf("[ERROR] Unrecognised database type during connection string lookup: %s", service)
	}

	connectionString = cdbConnection.(*ConnectionString)

	connectionString.Name = userName
	connectionString.Password = ""

	// Postgres DB name is of type string, Redis is json.Number, others are nil
	if connectionString.Database != nil {
		switch v := connectionString.Database.(type) {
		default:
			connectionString.Database = ""
			return *connectionString, fmt.Errorf("Unexpected data type: %T", v)
		case json.Number:
			connectionString.Database = connectionString.Database.(json.Number).String()
		case string:
			connectionString.Database = connectionString.Database.(string)
		}
	} else {
		connectionString.Database = ""
	}

	return *connectionString, nil
}

func getDatabaseServiceDefaults(service string, meta interface{}) (*clouddatabasesv5.Group, error) {
	cloudDatabasesV5, err := meta.(ClientSession).CloudDatabasesAPI()
	if err != nil {
		return nil, fmt.Errorf("[ERROR] Error getting database client settings: %s", err)
	}

	var dbType string
	if service == "databases-for-cassandra" {
		dbType = "datastax_enterprise_full"
	} else if strings.HasPrefix(service, "messages-for-") {
		dbType = service[len("messages-for-"):]
	} else {
		dbType = service[len("databases-for-"):]
	}

	getDefaultScalingGroupsOptions := &clouddatabasesv5.GetDefaultScalingGroupsOptions{
		Type: &dbType,
	}

	getDefaultScalingGroupsResponse, _, err := cloudDatabasesV5.GetDefaultScalingGroups(getDefaultScalingGroupsOptions)
	if err != nil {
		return nil, fmt.Errorf("ICD API is down for plan validation, set plan_validation=false %s", err)
	}
	return &getDefaultScalingGroupsResponse.Groups[0], nil
}

func getInitialNodeCount(d *schema.ResourceData, meta interface{}) (int, error) {
	service := d.Get("service").(string)
	planPhase := d.Get("plan_validation").(bool)
	if planPhase {
		groupDefaults, err := getDatabaseServiceDefaults(service, meta)
		if err != nil {
			return 0, err
		}
		return int(*groupDefaults.Members.MinimumCount), nil
	} else {
		if service == "databases-for-elasticsearch" {
			return 3, nil
		} else if service == "databases-for-cassandra" {
			return 3, nil
		}
		return 2, nil
	}
}

func expandAutoscalingDiskGroup(d *schema.ResourceData, asRecord interface{}) (autoscalingDiskGroup clouddatabasesv5.AutoscalingDiskGroupDisk, err error) {
	autoscalingRecord := asRecord.([]interface{})[0].(map[string]interface{})
	autoscalingDiskGroupCapacity := clouddatabasesv5.AutoscalingDiskGroupDiskScalersCapacity{}

	if _, ok := autoscalingRecord["capacity_enabled"]; ok {
		autoscalingDiskGroupCapacity.Enabled = core.BoolPtr(autoscalingRecord["capacity_enabled"].(bool))
		autoscalingDiskGroup.Scalers.Capacity = &autoscalingDiskGroupCapacity
	}
	if _, ok := autoscalingRecord["free_space_less_than_percent"]; ok {
		autoscalingDiskGroupCapacity.FreeSpaceLessThanPercent = core.Int64Ptr(autoscalingRecord["free_space_less_than_percent"].(int64))
		autoscalingDiskGroup.Scalers.Capacity = &autoscalingDiskGroupCapacity
	}

	// IO Payload
	autoscalingDiskIoUtilization := clouddatabasesv5.AutoscalingDiskGroupDiskScalersIoUtilization{}
	if _, ok := autoscalingRecord["io_enabled"]; ok {
		autoscalingDiskIoUtilization.Enabled = core.BoolPtr(autoscalingRecord["io_enabled"].(bool))
		autoscalingDiskGroup.Scalers.IoUtilization = &autoscalingDiskIoUtilization
	}
	if _, ok := autoscalingRecord["io_over_period"]; ok {
		autoscalingDiskIoUtilization.OverPeriod = core.StringPtr(autoscalingRecord["io_over_period"].(string))
		autoscalingDiskGroup.Scalers.IoUtilization = &autoscalingDiskIoUtilization
	}
	if _, ok := autoscalingRecord["io_above_percent"]; ok {
		autoscalingDiskIoUtilization.AbovePercent = core.Int64Ptr(autoscalingRecord["io_above_percent"].(int64))
		autoscalingDiskGroup.Scalers.IoUtilization = &autoscalingDiskIoUtilization
	}

	// Rate Payload
	autoscalingDiskGroupDiskRate := clouddatabasesv5.AutoscalingDiskGroupDiskRate{}
	if _, ok := autoscalingRecord["rate_increase_percent"]; ok {
		autoscalingDiskGroupDiskRate.IncreasePercent = core.Float64Ptr(autoscalingRecord["rate_increase_percent"].(float64))
		autoscalingDiskGroup.Rate = &autoscalingDiskGroupDiskRate
	}
	if _, ok := autoscalingRecord["rate_period_seconds"]; ok {
		autoscalingDiskGroupDiskRate.PeriodSeconds = core.Int64Ptr(autoscalingRecord["rate_period_seconds"].(int64))
		autoscalingDiskGroup.Rate = &autoscalingDiskGroupDiskRate
	}
	if _, ok := autoscalingRecord["rate_limit_mb_per_member"]; ok {
		autoscalingDiskGroupDiskRate.LimitMbPerMember = core.Float64Ptr(autoscalingRecord["rate_limit_mb_per_member"].(float64))
		autoscalingDiskGroup.Rate = &autoscalingDiskGroupDiskRate
	}
	if _, ok := autoscalingRecord["rate_units"]; ok {
		autoscalingDiskGroupDiskRate.Units = core.StringPtr(autoscalingRecord["rate_units"].(string))
		autoscalingDiskGroup.Rate = &autoscalingDiskGroupDiskRate
	}

	return autoscalingDiskGroup, nil
}

func expandAutoscalingMemoryGroup(d *schema.ResourceData, asRecord interface{}) (autoscalingMemoryGroup clouddatabasesv5.AutoscalingMemoryGroupMemory, err error) {
	autoscalingRecord := asRecord.([]interface{})[0].(map[string]interface{})

	// IO Payload
	autoscalingMemoryIoUtilization := clouddatabasesv5.AutoscalingMemoryGroupMemoryScalersIoUtilization{}
	if _, ok := autoscalingRecord["io_enabled"]; ok {
		autoscalingMemoryIoUtilization.Enabled = core.BoolPtr(autoscalingRecord["io_enabled"].(bool))
		autoscalingMemoryGroup.Scalers.IoUtilization = &autoscalingMemoryIoUtilization
	}
	if _, ok := autoscalingRecord["io_over_period"]; ok {
		autoscalingMemoryIoUtilization.OverPeriod = core.StringPtr(autoscalingRecord["io_over_period"].(string))
		autoscalingMemoryGroup.Scalers.IoUtilization = &autoscalingMemoryIoUtilization
	}
	if _, ok := autoscalingRecord["io_above_percent"]; ok {
		autoscalingMemoryIoUtilization.AbovePercent = core.Int64Ptr(autoscalingRecord["io_above_percent"].(int64))
		autoscalingMemoryGroup.Scalers.IoUtilization = &autoscalingMemoryIoUtilization
	}

	// Rate Payload
	autoscalingMemoryGroupMemoryRate := clouddatabasesv5.AutoscalingMemoryGroupMemoryRate{}
	if _, ok := autoscalingRecord["rate_increase_percent"]; ok {
		autoscalingMemoryGroupMemoryRate.IncreasePercent = core.Float64Ptr(autoscalingRecord["rate_increase_percent"].(float64))
		autoscalingMemoryGroup.Rate = &autoscalingMemoryGroupMemoryRate
	}
	if _, ok := autoscalingRecord["rate_period_seconds"]; ok {
		autoscalingMemoryGroupMemoryRate.PeriodSeconds = core.Int64Ptr(autoscalingRecord["rate_period_seconds"].(int64))
		autoscalingMemoryGroup.Rate = &autoscalingMemoryGroupMemoryRate
	}
	if _, ok := autoscalingRecord["rate_limit_mb_per_member"]; ok {
		autoscalingMemoryGroupMemoryRate.LimitMbPerMember = core.Float64Ptr(autoscalingRecord["rate_limit_mb_per_member"].(float64))
		autoscalingMemoryGroup.Rate = &autoscalingMemoryGroupMemoryRate
	}
	if _, ok := autoscalingRecord["rate_units"]; ok {
		autoscalingMemoryGroupMemoryRate.Units = core.StringPtr(autoscalingRecord["rate_units"].(string))
		autoscalingMemoryGroup.Rate = &autoscalingMemoryGroupMemoryRate
	}

	return autoscalingMemoryGroup, nil
}

func expandAutoscalingCPUGroup(d *schema.ResourceData, asRecord interface{}) (autoscalingCPUGroup clouddatabasesv5.AutoscalingCPUGroupCPU, err error) {
	autoscalingRecord := asRecord.([]interface{})[0].(map[string]interface{})

	// Rate Payload
	autoscalingCPUGroupCPURate := clouddatabasesv5.AutoscalingCPUGroupCPURate{}
	if _, ok := autoscalingRecord["rate_increase_percent"]; ok {
		autoscalingCPUGroupCPURate.IncreasePercent = core.Float64Ptr(autoscalingRecord["rate_increase_percent"].(float64))
		autoscalingCPUGroup.Rate = &autoscalingCPUGroupCPURate
	}
	if _, ok := autoscalingRecord["rate_period_seconds"]; ok {
		autoscalingCPUGroupCPURate.PeriodSeconds = core.Int64Ptr(autoscalingRecord["rate_period_seconds"].(int64))
		autoscalingCPUGroup.Rate = &autoscalingCPUGroupCPURate
	}
	if _, ok := autoscalingRecord["rate_limit_count_per_member"]; ok {
		autoscalingCPUGroupCPURate.LimitCountPerMember = core.Int64Ptr(autoscalingRecord["rate_limit_count_per_member"].(int64))
		autoscalingCPUGroup.Rate = &autoscalingCPUGroupCPURate
	}
	if _, ok := autoscalingRecord["rate_units"]; ok {
		autoscalingCPUGroupCPURate.Units = core.StringPtr(autoscalingRecord["rate_units"].(string))
		autoscalingCPUGroup.Rate = &autoscalingCPUGroupCPURate
	}

	return autoscalingCPUGroup, nil
}

func flattenICDAutoScalingGroup(autoScalingGroup clouddatabasesv5.AutoscalingGroup) []map[string]interface{} {
	result := make([]map[string]interface{}, 0)
	memorys := make([]map[string]interface{}, 0)
	memory := make(map[string]interface{})

	if autoScalingGroup.Autoscaling.Memory.Scalers.IoUtilization != nil {
		memoryIO := *autoScalingGroup.Autoscaling.Memory.Scalers.IoUtilization
		memory["io_enabled"] = memoryIO.Enabled
		memory["io_over_period"] = memoryIO.OverPeriod
		memory["io_above_percent"] = memoryIO.AbovePercent
	}
	if &autoScalingGroup.Autoscaling.Memory.Rate != nil {
		ip := autoScalingGroup.Autoscaling.Memory.Rate.IncreasePercent
		memory["rate_increase_percent"] = *ip
		memory["rate_period_seconds"] = autoScalingGroup.Autoscaling.Memory.Rate.PeriodSeconds
		lmp := autoScalingGroup.Autoscaling.Memory.Rate.LimitMbPerMember
		memory["rate_limit_mb_per_member"] = *lmp
		memory["rate_units"] = autoScalingGroup.Autoscaling.Memory.Rate.Units
	}
	memorys = append(memorys, memory)

	cpus := make([]map[string]interface{}, 0)
	cpu := make(map[string]interface{})

	if &autoScalingGroup.Autoscaling.CPU.Rate != nil {

		ip := autoScalingGroup.Autoscaling.CPU.Rate.IncreasePercent
		cpu["rate_increase_percent"] = *ip
		cpu["rate_period_seconds"] = autoScalingGroup.Autoscaling.CPU.Rate.PeriodSeconds
		cpu["rate_limit_count_per_member"] = autoScalingGroup.Autoscaling.CPU.Rate.LimitCountPerMember
		cpu["rate_units"] = autoScalingGroup.Autoscaling.CPU.Rate.Units
	}
	cpus = append(cpus, cpu)

	disks := make([]map[string]interface{}, 0)
	disk := make(map[string]interface{})
	if autoScalingGroup.Autoscaling.Disk.Scalers.Capacity != nil {
		diskCapacity := *autoScalingGroup.Autoscaling.Disk.Scalers.Capacity
		disk["capacity_enabled"] = diskCapacity.Enabled
		disk["free_space_less_than_percent"] = diskCapacity.FreeSpaceLessThanPercent
	}
	if autoScalingGroup.Autoscaling.Disk.Scalers.IoUtilization != nil {
		diskIO := *autoScalingGroup.Autoscaling.Disk.Scalers.IoUtilization
		disk["io_enabled"] = diskIO.Enabled
		disk["io_over_period"] = diskIO.OverPeriod
		disk["io_above_percent"] = diskIO.AbovePercent
	}
	if &autoScalingGroup.Autoscaling.Disk.Rate != nil {

		ip := autoScalingGroup.Autoscaling.Disk.Rate.IncreasePercent
		disk["rate_increase_percent"] = ip
		disk["rate_period_seconds"] = autoScalingGroup.Autoscaling.Disk.Rate.PeriodSeconds
		lpm := autoScalingGroup.Autoscaling.Disk.Rate.LimitMbPerMember
		disk["rate_limit_mb_per_member"] = lpm
		disk["rate_units"] = autoScalingGroup.Autoscaling.Disk.Rate.Units
	}

	disks = append(disks, disk)
	as := map[string]interface{}{
		"memory": memorys,
		"cpu":    cpus,
		"disk":   disks,
	}
	result = append(result, as)
	return result
}

func retry(f func() error) (err error) {
	attempts := 3

	for i := 0; ; i++ {
		sleep := time.Duration(10*i*i) * time.Second
		time.Sleep(sleep)

		err = f()
		if err == nil {
			return nil
		}

		if i == attempts {
			return err
		}

		log.Println("retrying after error:", err)
	}
}
