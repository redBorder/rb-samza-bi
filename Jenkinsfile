
node('runner') {
	stage 'Compilation'
	git branch: 'continuous-integration', credentialsId: 'jenkins_id', url: 'git@gitlab.redborder.lan:bigdata/rb-samza-bi.git'
	def mvnHome = tool 'M3'
	sh "${mvnHome}/bin/mvn clean package"
	stash includes: 'target/rb-samza-bi*.tar.gz', name: 'rb-samza-bi'
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

