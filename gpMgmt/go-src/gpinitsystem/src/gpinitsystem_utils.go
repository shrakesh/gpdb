package main

import (
	"errors"
	"fmt"
	"log"
	"os"
	"os/exec"
	"os/user"
	"strconv"
	"strings"
)

var gpArray struct{
	GpHostname string
	GpHostAddress string
	GpPort string // this can be more suitable if it is int
	GpDir string
	GpDbId string
	GpContent string
}

// function to check if the program is present on the machine or not.
// for this kind of function  we can have validate_depend() or init function would be best

// findCmdInPath
func findCmdInPath(cmd string){
	//to do
	// mostly this function is not required we can check it directly while using the perticular command
	// just to verify for the backout script and remote execution
	 exec.LookPath("gpstart")
}

// findMppPath
//func findMppPath() error{
//	// to do
//	// function to get the path of mpp function(gpstart/ gpstop)
//}

// IN_ARRAY this function from bash is not much of use
// we will be having log level set and based on that message can be writtern so function LOG_MSG is not required

// setGpArrayWithSlice function take slice as argument and update the gpArray
func setGpArrayWithSlice(fields []string,  fType int) (){

	switch fType {
	case 0:// old format
		gpArray.GpHostname = fields[0]
		gpArray.GpHostAddress = fields[0]
		gpArray.GpPort = fields[1]
		gpArray.GpDir = fields[2]
		gpArray.GpDbId = fields[3]
		gpArray.GpContent = fields[4]
	default:
		gpArray.GpHostname = fields[0]
		gpArray.GpHostAddress = fields[1]
		gpArray.GpPort = fields[2]
		gpArray.GpDir = fields[3]
		gpArray.GpDbId = fields[4]
		gpArray.GpContent = fields[5]
	}
}
// isValidGpArray validate the content of gpArray
// mostly used after parsing the element

func  isValidGpArray()(bool){
	//check type and validate if the content of the gpArray is good
	if _, err := strconv.Atoi(gpArray.GpPort); err != nil {
		fmt.Printf("port: %s is not a valid number.\n", gpArray.GpPort)
		return false
	}
	if _, err := strconv.Atoi(gpArray.GpDbId); err != nil {
		fmt.Printf("dbid: %s is not a valid number.\n", gpArray.GpDbId)
		return false
	}
	if _, err := strconv.Atoi(gpArray.GpContent); err != nil {
		fmt.Printf("content: %s is not a valid number.\n", gpArray.GpContent)
		return false
	}
	if _, err := os.Stat(gpArray.GpDir); err != nil {
		fmt.Printf("dir: %s is not a valid directory.\n", gpArray.GpDir)
		return false
	}

	return true
}

// getCurrentUserName get current user name
func getCurrentUserName() string {

	user, err := user.Current()
	if err != nil {
		log.Fatalf(err.Error())
	}
	return user.Username
}


// AssignCoordinatorVars
func AssignCoordinatorVars() error {
	// Use COORDINATOR_* variables by default, fall back to MASTER_* variables if necessary
	if _,ok := os.LookupEnv("COORDINATOR_HOSTNAME"); !ok{
		if host, ok := os.LookupEnv("MASTER_HOSTNAME"); ok{
			os.Setenv("COORDINATOR_HOSTNAME",host)
		}else{
			log.Fatalf("[FATAL]:-COORDINATOR_HOSTNAME variable not set")
			return errors.New("")
		}
	}

	if _,ok := os.LookupEnv("COORDINATOR_PORT"); !ok{
		if host, ok := os.LookupEnv("MASTER_PORT"); ok{
			os.Setenv("COORDINATOR_PORT",host)
		}else{
			log.Fatalf("[FATAL]:-COORDINATOR_PORT variable not set")
			return errors.New("")
		}
	}

	if _,ok := os.LookupEnv("COORDINATOR_DIRECTORY"); !ok{
		if host, ok := os.LookupEnv("MASTER_DIRECTORY"); ok{
			os.Setenv("COORDINATOR_DIRECTORY",host)
		}else{
			log.Fatalf("[FATAL]:-COORDINATOR_DIRECTORY variable not set")
			return errors.New("")
		}
	}
	return nil
}


// getQDArrayFormat function to check the format and update gpArray
func getQDArrayFormat(QE string) (string,error){
	separator := ""
	var IsDeprecatedFormatForQd bool
	if strings.ContainsAny(QE, "~"){
		separator = "~"
	}else{
		separator = ":"
	}
	if len(separator) == 0 || len(QE) == 0 {
		return "",errors.New("input string is not valid")
	}

	fields := strings.Split(QE, separator)

	//# The input_config format for specifying a segment array changed in a 6X
	//# minor release to include the hostname in addition to the address.  To
	//# maintain backwards compatibility, detect when the incoming array needs
	//# the host field to be prepended.  For example, an input line of
	//# QD_PRIMARY_ARRAY=mdw~5432~/data/master/gpseg-1~1~-1
	//# would be treated as
	//# QD_PRIMARY_ARRAY=mdw~mdw~5432~/data/master/gpseg-1~1~-1

	fieldSize := len(fields)

	if (fieldSize != 5) || (fieldSize != 6) {
		return "",errors.New("[FATAL]:-$I has the wrong number of fields")
	}

	//# Handle backward compatibility for configuration file generated
	//# which had ~0 at the end for QD_PRIMARY_ARRAY


	if fields[5] == "0" && fields[4] == "-1"{
		IsDeprecatedFormatForQd =true
	}

	if fieldSize == 5 && IsDeprecatedFormatForQd{
	//# Ex: mdw~5432~/data/master/gpseg-1~1~-1
	//# or
	//# mdw~5432~/data/master/gpseg-1~1~-1~0
		setGpArrayWithSlice(fields, 0 )
	}else{
	//# ARRAY for master / segments
	//# Ex: mdw~mdw~5432~/data/master/gpseg-1~1~-1
		setGpArrayWithSlice(fields, 1 )
	}

	if isValidGpArray() == false{
		return "",errors.New("Fatal error ")
	}

	return "", nil
}

//// errorExit
//func errorExit () (){
//	os.Exit()
//}

// checkParam
func checkParam()(error){
	log.Printf("[INFO]:-Checking configuration parameters, please wait...")
	user, err := user.Current()
	if err != nil {
		log.Fatalf(err.Error())
	}

	username := user.Username

	if os.Geteuid() == 0{
		error := fmt.Sprintf("[FATAL]:-Unable to run this script as root user: %s.",username )
		return errors.New(error)
	}

	if _,ok := os.LookupEnv("GPHOME"); !ok{
		log.Fatalf("[FATAL]:-Environment variable GPHOME not set")
		log.Fatalf("[FATAL]:-Unable to continue")
		return errors.New("")
	}

	if _, err = exec.LookPath("initdb"); err != nil{
		log.Fatalf("[FATAL]:-Unable to locate initdb")
		return errors.New("")
	}

	// check either cluster config or input config is supplied
	// To-Do

	clusterConfigFile := "clusterConfigFile"
	if clusterConfigFile != "" {

		ClusterConfigData, err :=  ReadClusterConfigFile( clusterConfigFile)
		if err != nil{
			return err
		}

		if err := AssignCoordinatorVars(); err != nil{
			return err
		}

		// check if $QD_PRIMARY_ARRAY is supplied with -c option
		// need to have discussion if we are not supplying do we really need to have this check

		if len(ClusterConfigData.Primary.PortBase) == 0{
			log.Fatalf("[FATAL]:-PORT_BASE not specified in %s file, is this the correct instance configuration file.",clusterConfigFile)
		}

		if len(ClusterConfigData.Mirror.PortBase) > 0 {
			// setMirroring
		}

		// imo unsetting variable is not required
		//os.Unsetenv("PORT_BASE")
		//os.Unsetenv("SEG_PREFIX")
		//os.Unsetenv("DATA_DIRECTORY")
		//os.Unsetenv("HEAP_CHECKSUM")
		//os.Unsetenv("HBA_HOSTNAMES")

		//Dumping cluster config file to log file for reference
	} else{
		inputConfigFile := "inputConfigFile"
		log.Printf("[INFO]:-Reading Greenplum input configuration file %s", inputConfigFile)

		InputConfigData, err :=  ReadInputConfigFile( inputConfigFile)
		if err != nil{
			return err
		}


	}


}
