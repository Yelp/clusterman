@Library('jenkinsfile_stdlib') _
import com.yelpcorp.releng.Utils
import com.yelpcorp.releng.EEMetrics

yproperties()
utils = new Utils()
eeMetrics = new EEMetrics()

SERVICE_NAME = 'clusterman'
DEPLOY_GROUPS = ['other-batches', 'dev-stage-testopia.default', 'testopia.jolt', 'prod.everything']
IRC_CHANNELS = ['clusterman']
EMAILS = ['compute-infra@yelp.com']

commit = ''
authors = [:]

node {
    eeMetrics.emitEvent("${env.job_name}", 'jenkins', "${env.job_name}-${env.build_id}", 'start')
}

utils.handleInputRejection {
    ircMsgResult(IRC_CHANNELS) {
        emailResult(EMAILS) {
            node('trusty') {
                ystage('clone') {
                    commit = clone("services/${SERVICE_NAME}")['GIT_COMMIT']
                    eeMetrics.emitLink('sha', "${commit}", 'jenkins', "${env.JOB_NAME}-${env.BUILD_ID}")

                    for (deploy_group in DEPLOY_GROUPS) {
                        current = sh(script: "paasta get-latest-deployment --service ${SERVICE_NAME} --deploy-group ${deploy_group} || git hash-object -t tree /dev/null", returnStdout: true).trim()
                        author = sh(script: "git log --format=%ae ${current}..${commit} | sort -u | cut -d@ -f1 | xargs --no-run-if-empty", returnStdout: true).trim()

                        authors[deploy_group] = author.tokenize()
                    }
                }

                ystage(eeMetricsWorkflow: 'test', 'test') {
                    sh(script: 'make test')
                }

                ystage('itest') {
                    sh(script: $/paasta itest --service ${SERVICE_NAME} --commit ${commit}/$)
                }

                ystage('security-check') {
                    try {
                        sh(script: $/paasta security-check --service services-${SERVICE_NAME} --commit ${commit}/$)
                    } catch (hudson.AbortException e) {
                        // Do nothing
                    }
                }

                ystage('push-to-registry') {
                    sh(script: $/paasta push-to-registry --service ${SERVICE_NAME} --commit ${commit}/$)
                }

                ystage('performance-check') {
                    sh(script: $/paasta performance-check --service ${SERVICE_NAME}/$)
                }

                ystage('debian-upload') {
                    // Runs `make itest_${version}` and attempts to upload to apt server if not an automatically timed run
                    // This will automatically break all the steps into stages for you
                    //
                    // We do networking with docker-compose and the networks conflict so we have to do each version separately
                    debItestUpload("services/${SERVICE_NAME}", ['trusty'])
                    debItestUpload("services/${SERVICE_NAME}", ['xenial'])

                    if (!wasTimerTriggered() && authors['prod.everything']) {
                        pingList = authors['prod.everything'].split(' ').collect{author -> "<@${author}>"}.join(', ')
                        utils.nodebot(IRC_CHANNELS, "Hey ${pingList}, go click the button! :easy_button: y/clusterman-jenkins")
                    }
                    timeout(time: 1, unit: 'HOURS') { input "Click to advance to next step" }
                }
            }

            ystage('other-batches') {
                paastaDeploy(SERVICE_NAME, commit, 'other-batches', waitForDeployment: true, confirmation: false, deployTimeout: false, autoRollback: false, productionDeploy: false)
            }

            ystage('dev-stage-testopia.default') {
                paastaDeploy(SERVICE_NAME, commit, 'dev-stage-testopia.default', waitForDeployment: true, confirmation: false, deployTimeout: true, autoRollback: false, productionDeploy: false)
            }

            ystage('testopia.jolt') {
                paastaDeploy(SERVICE_NAME, commit, 'testopia.jolt', waitForDeployment: true, confirmation: true, deployTimeout: false, autoRollback: false, productionDeploy: false)
            }

            ystage('prod.everything') {
                paastaDeploy(SERVICE_NAME, commit, 'prod.everything', waitForDeployment: true, confirmation: true, deployTimeout: false, autoRollback: false, productionDeploy: true)
            }
        }
    }

    node {
        eeMetrics.emitEvent("${env.job_name}", 'jenkins', "${env.job_name}-${env.build_id}", 'end')
    }
}
