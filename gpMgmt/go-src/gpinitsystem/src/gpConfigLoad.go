package main

import (
	"errors"
	"fmt"
	toml "github.com/pelletier/go-toml"
	"log"
	"os"
	"os/exec"
	"os/user"
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


func getCurrentUserName() string {

	user, err := user.Current()
	if err != nil {
		log.Fatalf(err.Error())
	}
	return user.Username
}

// readClusterConfigFile
func readClusterConfigFile( clusterConfigFile string) (ClusterConfig, error){

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

	if _, err = exec.LookPath("initdb"); err != nil{
		log.Fatalf("[FATAL]:-Unable to locate initdb")
		return configFile,errors.New("")
	}

	config, err := toml.LoadFile(clusterConfigFile)

	if err != nil {
		fmt.Println("Error ", err.Error())
	} else {
		// retrieve data directly
		configFile.ArrayName = config.Get("ARRAY_NAME").(string)
		configFile.MachineListFile = config.Get("MACHINE_LIST_FILE").(string)
		configFile.SegPrefix = config.Get("SEG_PREFIX").(string)
		configFile.TrustedShell = config.Get("TRUSTED_SHELL").(string)
		configFile.CheckPointSegments = config.Get("CHECK_POINT_SEGMENTS").(string)
		configFile.Encoding = config.Get("ENCODING").(string)
		configFile.DefaultQDMaxConnect = config.Get("DEFAULT_QD_MAX_CONNECT").(string)
		configFile.QEConnectFactor = config.Get("QE_CONNECT_FACTOR").(string)

		// setting up master data
		configFile.Master.HostName = config.Get("MASTER_HOSTNAME").(string)
		configFile.Master.DataDirectory = config.Get("MASTER_DIRECTORY").(string)
		configFile.Master.Port = config.Get("MASTER_PORT").(string)

		// setting up standby data
		configFile.Standby.HostName = config.Get("STANDBY_HOSTNAME").(string)
		configFile.Standby.DataDirectory = config.Get("STANDBY_DIRECTORY").(string)
		configFile.Standby.Port = config.Get("STANDBY_PORT").(string)

		//setting up primary data
		configFile.Primary.PortBase= config.Get("PORT_BASE").(string)
		PrimaryDataDir := config.Get("DATA_DIRECTORY").(string)
		PrimaryDataDir = strings.TrimPrefix(PrimaryDataDir,"(")
		PrimaryDataDir = strings.TrimSuffix(PrimaryDataDir,")")
		configFile.Primary.DataDirectory = strings.Split(PrimaryDataDir," ")

		//setting up mirror data
		configFile.Mirror.PortBase= config.Get("MIRROR_PORT_BASE").(string)
		MirrorDataDir := config.Get("MIRROR_DATA_DIRECTORY").(string)
		MirrorDataDir = strings.TrimPrefix(MirrorDataDir,"(")
		MirrorDataDir = strings.TrimSuffix(MirrorDataDir,")")
		configFile.Mirror.DataDirectory = strings.Split(MirrorDataDir," ")
	}
	//Dumping cluster config file to log file for reference
	return configFile, nil
}