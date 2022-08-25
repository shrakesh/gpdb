package main

import (
	"bufio"
	"errors"
	"fmt"
	"log"
	"os"
	"os/exec"
	"strings"
)

// ClusterConfig contains all the configuration option
type ClusterConfig struct {
	ArrayName  string
	MachineListFile  string
	SegPrefix  string
	TrustedShell string
	CheckPointSegments string
	Encoding   string
	DefaultQDMaxConnect string
	QEConnectFactor string
	Master      MasterConfig
	Standby    StandByConfig
	Primary    PrimaryConfig
	Mirror     MirrorConfig
}

// ServerConfigurations exported
type MasterConfig struct {
	Port string
	DataDirectory string
	HostName string
}

type StandByConfig struct {
	Port string
	DataDirectory string
	HostName string
}

type PrimaryConfig struct {
	PortBase string
	DataDirectory []string
}

type MirrorConfig struct {
	PortBase string
	DataDirectory []string
}

// parseConfigFile Parse cluster config file and return key val pair
func parseClusterConfigFile(filename string) (map[string]string, error){
	file, err := os.Open(filename)
	keyVal := make(map[string] string)

	if err != nil{
		log.Fatalf("[FATAL]:- unable to read the file")
		return keyVal, errors.New("")
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.Trim(scanner.Text(),"\n")
		if len(line) == 0  || strings.HasPrefix(line,"#"){
			continue
		}
		fields := strings.Split(line,"=")
		key := strings.TrimPrefix(fields[0], "declare -a")
		key = strings.TrimSpace(key)
		val := strings.TrimSpace(fields[1])

		keyVal[key] = strings.TrimSpace(val)
	}

	return keyVal, nil
}

// ReadClusterConfigFile function to read file
func ReadClusterConfigFile( clusterConfigFile string) (ClusterConfig, error){

	configFile := ClusterConfig{}

	log.Printf("[INFO]:-Checking configuration parameters, please wait...")

	// check if error is "file not exists"
	if _ , err := os.Stat(clusterConfigFile); os.IsNotExist(err) {
		log.Fatalf("[FATAL]:-Configuration file %s does not exist.",clusterConfigFile)
		return configFile,errors.New("")
	}

	user := getCurrentUserName()

	if os.Geteuid() == 0{
		error := fmt.Sprintf("[FATAL]:-Unable to run this script as root user: %s.",user )
		return configFile,errors.New(error)
	}

	if _,ok := os.LookupEnv("GPHOME"); !ok{
		log.Fatalf("[FATAL]:-Environment variable GPHOME not set")
		log.Fatalf("[FATAL]:-Unable to continue")
		return configFile,errors.New("")
	}

	if _, err := exec.LookPath("initdb"); err != nil{
		log.Fatalf("[FATAL]:-Unable to locate initdb")
		return configFile,errors.New("")
	}

	tokenMap, ParseErr := parseClusterConfigFile(clusterConfigFile)
	if ParseErr != nil {
		return configFile,ParseErr
	}

	configFile.ArrayName = tokenMap["ARRAY_NAME"]
	configFile.MachineListFile = tokenMap["MACHINE_LIST_FILE"]
	configFile.SegPrefix = tokenMap["SEG_PREFIX"]
	configFile.TrustedShell = tokenMap["TRUSTED_SHELL"]
	configFile.CheckPointSegments = tokenMap["CHECK_POINT_SEGMENTS"]
	configFile.Encoding = tokenMap["ENCODING"]
	configFile.DefaultQDMaxConnect = tokenMap["DEFAULT_QD_MAX_CONNECT"]
	configFile.QEConnectFactor = tokenMap["QE_CONNECT_FACTOR"]

	// setting up master data
	configFile.Master.HostName = tokenMap["MASTER_HOSTNAME"]
	configFile.Master.DataDirectory = tokenMap["MASTER_DIRECTORY"]
	configFile.Master.Port = tokenMap["MASTER_PORT"]

	// setting up standby data
	configFile.Standby.HostName = tokenMap["STANDBY_HOSTNAME"]
	configFile.Standby.DataDirectory = tokenMap["STANDBY_DIRECTORY"]
	configFile.Standby.Port = tokenMap["STANDBY_PORT"]

	//setting up primary data
	configFile.Primary.PortBase= tokenMap["PORT_BASE"]
	PrimaryDataDir := tokenMap["DATA_DIRECTORY"]
	PrimaryDataDir = strings.TrimPrefix(PrimaryDataDir,"(")
	PrimaryDataDir = strings.TrimSuffix(PrimaryDataDir,")")
	configFile.Primary.DataDirectory = strings.Split(PrimaryDataDir," ")

	//setting up mirror data
	configFile.Mirror.PortBase= tokenMap["MIRROR_PORT_BASE"]
	MirrorDataDir := tokenMap["MIRROR_DATA_DIRECTORY"]
	MirrorDataDir = strings.TrimPrefix(MirrorDataDir,"(")
	MirrorDataDir = strings.TrimSuffix(MirrorDataDir,")")
	configFile.Mirror.DataDirectory = strings.Split(MirrorDataDir," ")

	return configFile, nil
}