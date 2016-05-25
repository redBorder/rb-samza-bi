
node('docker') {
	stage 'Compilation'
        def maven = docker.image('maven:latest')
	maven.inside('-v /tmp/docker-m2cache:/root/.m2:rw'){
		git branch: 'continuous-integration', credentialsId: 'jenkins_id', url: 'git@gitlab.redborder.lan:bigdata/rb-samza-bi.git'
		sh 'mvn clean install'
		stash includes: 'target/rb-samza-bi*.tar.gz', name: 'rb-samza-bi'
	}
}

node('jenkins-premanager1') {
	stage 'Copy to preproduction'
	unstash 'rb-samza-bi'
	stage 'Update service'
	sh 'rm -f /opt/rb/var/chef/cookbooks/redBorder-manager/files/default/rb-samza-bi.tar.gz'
	sh 'cp target/rb-samza-bi*.tar.gz /opt/rb/var/chef/cookbooks/redBorder-manager/files/default/rb-samza-bi.tar.gz'
	sh '/opt/rb/bin/rb_upload_cookbooks.sh manager'
	sh '/opt/rb/bin/rb_wakeup_chef -c'
}

