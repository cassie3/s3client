#!/usr/bin/env bash
set -o xtrace
set -o errexit

s3url=$1
test_dir=$2
# edit .aws/credential default ak and sk firstly
aws=$(which aws)
aws_cli="$aws s3api --output text   --endpoint $s3url "
bucket="${test_dir##*/}"
TEST_TXT="this is a test"
TEST_RESULT_OP="/tmp/fuse-test.log"

function log_info(){
    fun_name=$1
    message=$2
    echo "$(date) - ${fun_name} - $message" >> ${TEST_RESULT_OP}
}

function test_01_mkdir(){
    DIR_NAME="DIR$(cat /dev/urandom | head -c 5 | md5sum | head -c 5)"
    mkdir ${DIR_NAME}
    if [ ! -d ${DIR_NAME} ]; then
        log_info $FUNCNAME "Directory ${DIR_NAME} was not created"
        echo "$FUNCNAME    --Fail"
        return 1
    fi

    ${aws_cli} list-objects --bucket ${bucket} |grep "${DIR_NAME}/" >/dev/null
    [[ $? != 0 ]] &&{
        log_info $FUNCNAME "Directory ${DIR_NAME} was not found on s3"
        echo "$FUNCNAME    --Fail"
        return 1
    }
    echo "$FUNCNAME    --Success"
    return 0

}

function test_02_cpdir
{
    DIR_NAME="DIR$(cat /dev/urandom | head -c 5 | md5sum | head -c 5)"
    DIR_NAME1="DIR$(cat /dev/urandom | head -c 5 | md5sum | head -c 5)"
    mkdir /opt/${DIR_NAME}
    mkdir /opt/${DIR_NAME1}
    date > /opt/${DIR_NAME1}/file1
    message1=$(cp -r /opt/${DIR_NAME} . 2>&1)
    if [[ $? != 0 || ! -d ${DIR_NAME} ]]; then
        log_info $FUNCNAME "Directory ${DIR_NAME} was not copy succeed,error: ${message1}"
        echo "$FUNCNAME    --Fail"
        return 1
    fi
    message2=$(cp -r /opt/${DIR_NAME1} . 2>&1)
    if [[ $? != 0 || ! -d ${DIR_NAME1} ]]; then
        log_info $FUNCNAME "Directory ${DIR_NAME1} was not copy succeed,error: ${message2}"
        echo "$FUNCNAME    --Fail"
        return 0
    fi

    ${aws_cli} list-objects --bucket ${bucket} |grep "${DIR_NAME}/" >/dev/null
    [[ $? != 0 ]] &&{
        log_info $FUNCNAME "Directory ${DIR_NAME} was not found on s3"
        echo "$FUNCNAME    --Fail"
        return 1
    }

    ${aws_cli} list-objects --bucket ${bucket} |grep "${DIR_NAME1}/" >/dev/null
    [[ $? != 0 ]] &&{
        log_info $FUNCNAME "Directory ${DIR_NAME1} was not found on s3"
        echo "$FUNCNAME    --Fail"
        return 1
    }
    echo "$FUNCNAME    --Success"
    rm -fr ${DIR_NAME1} ${DIR_NAME}
    return 0
}

function test_03_mvdir
{
    DIR_NAME="DIR$(cat /dev/urandom | head -c 5 | md5sum | head -c 5)"
    mkdir -p /opt/${DIR_NAME}
    message=$(mv  /opt/${DIR_NAME} . 2>&1)
    if [[ $? != 0 || ! -d ${DIR_NAME} ]]; then
        log_info $FUNCNAME "Directory ${DIR_NAME} was not mv succeed, error: ${message}"
        echo "$FUNCNAME    --Fail"
        resturn 1
    fi

    ${aws_cli} list-objects --bucket ${bucket} |grep "${DIR_NAME}/" >/dev/null
    [[ $? != 0 ]] &&{
        log_info $FUNCNAME "Directory ${DIR_NAME} was not found on s3"
        echo "$FUNCNAME    --Fail"
        return 1
    }
    echo "$FUNCNAME    --Success"
    return 0
}

function test_04_mkdir_depth
{
    test_dir="dir$(cat /dev/urandom | head -c 5 | md5sum | head -c 5)"
    DIR_NAME="${test_dir}/${test_dir}/${test_dir}/${test_dir}/${test_dir}/${test_dir}"
    mkdir -p ${DIR_NAME}
    if [ ! -d ${DIR_NAME} ]; then
        log_info $FUNCNAME "file ${DIR_NAME} was not create succeed"
        echo "$FUNCNAME    --Fail"
        return 1
    fi

    ${aws_cli} list-objects --bucket ${bucket} |grep "${DIR_NAME}/" >/dev/null
    [[ $? != 0 ]] &&{
        log_info $FUNCNAME "file ${DIR_NAME} was not found on s3"
        echo "$FUNCNAME    --Fail"
        return 1
    }
    echo "$FUNCNAME    --Success"
}

function test_05_touch_file
{
    filename="test.txt"
    touch ${filename}
    if [ ! -f ${filename} ]; then
        log_info $FUNCNAME "file ${filename} was not touch succeed"
        echo "$FUNCNAME    --Fail"
        return 1
    fi
    ${aws_cli} list-objects --bucket ${bucket} |grep "${filename}" >/dev/null
    [[ $? != 0 ]] &&{
        log_info $FUNCNAME "file ${filename} was not found on s3"
        echo "$FUNCNAME    --Fail"
        return 1
    }
    echo "$FUNCNAME    --Success"
    return 0
}

function test_06_echo_file
{
    filename="test$(cat /dev/urandom | head -c 5 | md5sum | head -c 5).txt"
    tmp_file="/tmp/getobject"
    echo "${TEST_TXT}" > ${filename}
    if [ ! -f ${filename} ]; then
        log_info $FUNCNAME "file ${filename} was not create succeed"
        echo "$FUNCNAME    --Fail"
        return 1
    fi
    ${aws_cli} get-object --bucket ${bucket} --key ${filename} ${tmp_file} > \
    /dev/null
    [[ $? != 0 ]] &&{
        log_info $FUNCNAME "file ${filename} was not found on s3"
        echo "$FUNCNAME    --Fail"
        return 1
    }
    #cmp <(echo "${TEST_TXT}") ${tmp_file}
    [[ "${TEST_TXT}" != $(cat ${tmp_file}) ]] && {
        log_info $FUNCNAME "file ${filename} was not found on s3"
        echo "$FUNCNAME    --Fail"
        return 1
     }
     echo "$FUNCNAME    --Success"
     rm -fr ${filename}
     return 0
}

function test_07_cp_file
{
    filename="test$(cat /dev/urandom | head -c 5 | md5sum | head -c 5).txt"
    tmp_file="/tmp/getobject"
    echo "${TEST_TXT}" > /tmp/${filename}
    message=$(cp  /tmp/${filename} . 2>&1)
    if [ ! -f ${filename} ]; then
        log_info $FUNCNAME "file ${filename} was not cp succeed, error: ${message}"
        echo "$FUNCNAME    --Fail"
        return 1
    fi
    ${aws_cli} get-object --bucket ${bucket} --key ${filename} ${tmp_file} > \
    /dev/null
    [[ $? != 0 ]] &&{
        log_info $FUNCNAME "file ${filename} was not found on s3"
        echo "$FUNCNAME    --Fail"
        return 1
    }
    #cmp <(echo "${TEST_TXT}") ${tmp_file}
    #[[ $? != 0 ]] && {
    [[ "${TEST_TXT}" != $(cat ${tmp_file}) ]] && {
        log_info $FUNCNAME "file ${filename} on s3 was not same as local"
        echo "$FUNCNAME    --Fail"
        return 1
     }

     mkdir -p dir1
     message=$(cp /tmp/${filename} dir1/ 2>&1)
     if [ ! -f dir1/${filename} ]; then
        log_info $FUNCNAME "file ${filename} was not cp succeed, error: ${message}"
        echo "$FUNCNAME    --Fail"
        return 1
    fi
    ${aws_cli} get-object --bucket ${bucket} --key dir1/${filename} ${tmp_file} > \
    /dev/null
    [[ $? != 0 ]] &&{
        log_info $FUNCNAME "file dir1/${filename} was not found on s3"
        echo "$FUNCNAME    --Fail"
        return 1
    }
    #cmp <(echo "${TEST_TXT}") ${tmp_file}
    #[[ $? != 0 ]] && {
    [[ "${TEST_TXT}" != $(cat ${tmp_file})  ]] && {
        log_info $FUNCNAME "file dir1/${filename} on s3 was not same as local"
        echo "$FUNCNAME    --Fail"
        return 1
     }
     echo "$FUNCNAME    --Success"
     rm -fr ${filename} dir1
     return 0
}

function test_08_mv_file
{
    filename="test$(cat /dev/urandom | head -c 5 | md5sum | head -c 5).txt"
    tmp_file="/tmp/getobject"
    echo "${TEST_TXT}" > /tmp/${filename}

    message=$(mv /tmp/${filename} . 2>&1)
    if [[ $? != 0 || ! -f dir1/${filename} ]]; then
        log_info $FUNCNAME "file ${filename} was not mv succeed, error: ${message}"
        echo "$FUNCNAME    --Fail"
        return 1
    fi
    ${aws_cli} get-object --bucket ${bucket} --key ${filename} ${tmp_file} > \
    /dev/null
    [[ $? != 0 ]] &&{
        log_info $FUNCNAME "file ${filename} was not found on s3"
        echo "$FUNCNAME    --Fail"
        return 1
    }
    #cmp <(echo "${TEST_TXT}") ${tmp_file}
    #[[ $? != 0 ]] && {
    [[ "${TEST_TXT}" != $(cat ${tmp_file}) ]] && {
        log_info $FUNCNAME "file ${filename} on s3 was not same as local"
        echo "$FUNCNAME    --Fail"
        return 1
     }

    mkdir -p dir1
    echo "${TEST_TXT}" > /tmp/${filename}
    message=$(mv /tmp/${filename} dir1/ 2>&1)
    if [[ $? != 0 || ! -f dir1/${filename} ]]; then
        log_info $FUNCNAME "file dir1/${filename} was not mv succeed, error: ${message}"
        echo "$FUNCNAME    --Fail"
        return 1
    fi
    ${aws_cli} get-object --bucket ${bucket} --key dir1/${filename} ${tmp_file} > \
    /dev/null
    [[ $? != 0 ]] &&{
        log_info $FUNCNAME "file dir1/${filename} was not found on s3"
        echo "$FUNCNAME    --Fail"
        return 1
    }
    #cmp <(echo "${TEST_TXT}") ${tmp_file}
    #[[ $? != 0 ]] && {
    [[ "${TEST_TXT}" != $(cat ${tmp_file}) ]] && {
        log_info $FUNCNAME "file dir1/${filename} on s3 was not same as local"
        echo "$FUNCNAME    --Fail"
        return 1
     }

     echo "$FUNCNAME    --Success"
     rm -fr dir1 ${filename}
     return 0
}

function test_09_larger_file
{
    filename="test.txt"
    dd if=/dev/urandom of=${filename} bs=1M count=25 iflag=fullblock   >/dev/null 2>&1
    tmp_file="/tmp/getobject"
    echo "${TEST_TXT}" > ${filename}
    if [ ! -f ${filename} ]; then
        log_info $FUNCNAME "file ${filename} was not create succeed"
        echo "$FUNCNAME    --Fail"
        return 1
    fi
    ${aws_cli} get-object --bucket ${bucket} --key ${filename} ${tmp_file} > \
    /dev/null
    [[ $? != 0 ]] &&{
        log_info $FUNCNAME "file ${filename} was not found on s3"
        echo "$FUNCNAME    --Fail"
        return 1
    }
    # <(echo "${TEST_TXT}") ${tmp_file}
    #[[ $? != 0 ]] && {
    [[ "${TEST_TXT}" != $(cat ${tmp_file}) ]] && {
        log_info $FUNCNAME "file ${filename} on s3 was not same as local"
        echo "$FUNCNAME    --Fail"
        return 1
     }
     echo "$FUNCNAME    --Success"
     rm -fr ${filename}
     return 0
}

function test_10_internal_cp_file
{
    filename="test$(cat /dev/urandom | head -c 5 | md5sum | head -c 5).txt"
    tmp_file="/tmp/getobject"

    date > ${filename}
    message=$(cp ${filename} ${filename}_cp 2>&1)
    if [ ! -f ${filename}_cp ]; then
        log_info $FUNCNAME "file ${filename}_cp was not cp succeed, error: ${message}"
        echo "$FUNCNAME    --Fail"
        return 1
    fi
    ${aws_cli} get-object --bucket ${bucket} --key ${filename}_cp ${tmp_file} > \
    /dev/null  2>&1
    [[ $? != 0 ]] &&{
        log_info $FUNCNAME "file ${filename}_cp was not found on s3"
        echo "$FUNCNAME    --Fail"
        return 1
    }
    cmp ${filename} ${tmp_file}
    [[ $? != 0 ]] && {
        log_info $FUNCNAME "file ${filename}_cp on s3 was not same as local"
        echo "$FUNCNAME    --Fail"
        return 1
     }

    mkdir -p dir1
    message=$(cp ${filename} dir1/ 2>&1)
    if [ ! -f dir1/${filename} ]; then
        log_info $FUNCNAME "file dir1/${filename} was not cp succeed, error: ${message}"
        echo "$FUNCNAME    --Fail"
        return 1
    fi
    ${aws_cli} get-object --bucket ${bucket} --key dir1/${filename} ${tmp_file} > \
    /dev/null >/dev/null 2>&1
    [[ $? != 0 ]] &&{
        log_info $FUNCNAME "file dir1/${filename} was not found on s3"
        echo "$FUNCNAME    --Fail"
        return 1
    }
    cmp ${filename} ${tmp_file}
    [[ $? != 0 ]] && {
        log_info $FUNCNAME "file dir1/${filename} on s3 was not same as local"
        echo "$FUNCNAME    --Fail"
        return 1
     }
     echo "$FUNCNAME    --Success"
     rm -fr ${filename}.* dir1
     return 0
}

function test_11_list_object
{
    filename="test$(cat /dev/urandom | head -c 5 | md5sum | head -c 5).txt"
    echo "${TEST_TXT}" > /tmp/${filename}
    ${aws_cli} put-object --bucket ${bucket} --key ${filename} --body /tmp/${filename} >/dev/null 2>&1
    [[ $? != 0 ]] &&{
        log_info $FUNCNAME "put ${filename} on s3 failed"
        echo "$FUNCNAME    --Fail"
        return 1
    }
    sleep 60
    if [ ! -f ${filename} ]; then
        log_info $FUNCNAME "file ${filename} was not existed on local"
        echo "$FUNCNAME    --Fail"
        return 1
    fi
    cmp ${filename} /tmp/${filename}
    [[ $? != 0 ]] && {
        log_info $FUNCNAME "file dir1/${filename} on s3 was not same as local"
        echo "$FUNCNAME    --Fail"
        return 1
     }
    echo "$FUNCNAME    --Success"

}

function test_12_external_cp_dir
{
    DIR_NAME="DIR$(cat /dev/urandom | head -c 5 | md5sum | head -c 5)"
    DIR_NAME1="DIR$(cat /dev/urandom | head -c 5 | md5sum | head -c 5)"
    mkdir -p ${DIR_NAME}
    mkdir -p ${DIR_NAME1}
    date > ${DIR_NAME1}/file1
    message=$(cp -r ${DIR_NAME} /tmp/${DIR_NAME} 2>&1)
    if [[ $? != 0 || ! -d /tmp/${DIR_NAME} ]]; then
        log_info $FUNCNAME "Directory /tmp/${DIR_NAME} was not copy succeed, error: ${message}"
        echo "$FUNCNAME    --Fail"
        return 1
    fi
    message=$(cp -r ${DIR_NAME1}  /tmp/${DIR_NAME1}.2>&1)
    if [  ! -d /tmp/${DIR_NAME1} ]; then
        log_info $FUNCNAME "Directory /tmp/${DIR_NAME1} was not copy succeed, error: ${message}"
        echo "$FUNCNAME    --Fail"
        return 0
    fi
    cmp ${DIR_NAME1}/file1 /tmp/${DIR_NAME1}/file1
    [[ $? != 0 ]] && {
        log_info $FUNCNAME "file dir1/${DIR_NAME1}/file1 was not same as s3"
        echo "$FUNCNAME    --Fail"
        return 1
     }
    echo "$FUNCNAME    --Success"
    rm -fr ${DIR_NAME1} ${DIR_NAME} /tmp/${DIR_NAME1} /tmp/${DIR_NAME}
    return 0
}

function test_13_external_mv_dir
{
    DIR_NAME="DIR$(cat /dev/urandom | head -c 5 | md5sum | head -c 5)"
    DIR_NAME1="DIR$(cat /dev/urandom | head -c 5 | md5sum | head -c 5)"
    mkdir -p ${DIR_NAME}
    mkdir -p ${DIR_NAME1}
    date > ${DIR_NAME1}/file1
    message=$(mv  ${DIR_NAME} /tmp/${DIR_NAME} 2>&1)
    if [[ $? != 0 || ! -d /tmp/${DIR_NAME} ]]; then
        log_info $FUNCNAME "Directory /tmp/${DIR_NAME} was not mv succeed, error: ${message}"
        echo "$FUNCNAME    --Fail"
        return 1
    fi
    if [ -d ${DIR_NAME} ]; then
        log_info $FUNCNAME "Directory ${DIR_NAME} was still exist"
    fi
    message=$(mv -r ${DIR_NAME1} /tmp/${DIR_NAME1} 2>&1)
    if [  ! -d /tmp/${DIR_NAME1} ]; then
        log_info $FUNCNAME "Directory /tmp/${DIR_NAME1} was not mv succeed, error: ${message}"
        echo "$FUNCNAME    --Fail"
        return 0
    fi

    if [ -d ${DIR_NAME1} ]; then
        log_info $FUNCNAME "Directory ${DIR_NAME1} was still exist"
    fi
    cmp ${DIR_NAME1}/file1 /tmp/${DIR_NAME1}/file1
    [[ $? != 0 ]] && {
        log_info $FUNCNAME "file dir1/${DIR_NAME1}/file1 was not same as s3"
        echo "$FUNCNAME    --Fail"
        return 1
     }
    echo "$FUNCNAME    --Success"
    rm -fr ${DIR_NAME1} ${DIR_NAME} /tmp/${DIR_NAME1} /tmp/${DIR_NAME}
    return 0
}

function test_14_external_cp_file
{
    DIR_NAME="DIR$(cat /dev/urandom | head -c 5 | md5sum | head -c 5)"
    filename="file$(cat /dev/urandom | head -c 5 | md5sum | head -c 5)"
    date > ${filename}
    message=$(cp ${filename} /tmp/ 2>&1 )
    if [[ $? != 0 || ! -f /tmp/${filename} ]]; then
        log_info $FUNCNAME "File /tmp/${filename} was not cp succeed, error: ${message}"
        echo "$FUNCNAME    --Fail"
        return 1
    fi
    cmp /tmp/${filename} ${filename}
    [[ $? != 0 ]] && {
        log_info $FUNCNAME "File /tmp/${filename} was not same as s3"
        echo "$FUNCNAME    --Fail"
        return 1
    }

    mkdir -p ${DIR_NAME}
    date > ${DIR_NAME}/${filename}
    message=$(cp ${DIR_NAME}/${filename} /tmp/${filename}_new 2>&1)
    if [[ $? != 0 || ! -f /tmp/${filename}_new ]]; then
        log_info $FUNCNAME "File /tmp/${filename}_new was not cp succeed, error: ${message}"
        echo "$FUNCNAME    --Fail"
        return 1
    fi
    cmp /tmp/${filename}_new  ${DIR_NAME}/${filename}
    [[ $? != 0 ]] && {
        log_info $FUNCNAME "File /tmp/${filename} was not same as s3"
        echo "$FUNCNAME    --Fail"
        return 1
    }
    echo "$FUNCNAME    --Success"
    rm -fr /tmp/${filename}.* ${DIR_NAME} ${filename}
}

function test_15_external_mv_file
{
    DIR_NAME="DIR$(cat /dev/urandom | head -c 5 | md5sum | head -c 5)"
    filename="file$(cat /dev/urandom | head -c 5 | md5sum | head -c 5)"
    echo "${TEST_TXT}"> ${filename}
    message=$(mv ${filename} /tmp/ 2>&1 )
    if [[ $? != 0 || ! -f /tmp/${filename} ]]; then
        log_info $FUNCNAME "File /tmp/${filename} was not mv succeed, error: ${message}"
        echo "$FUNCNAME    --Fail"
        return 1
    fi
    #cmp <(echo "${TEST_TXT}") ${filename}
    #[[ $? != 0 ]] && {
    [[ "${TEST_TXT}" != $(cat /tmp/${filename}) ]] && {
        log_info $FUNCNAME "File /tmp/${filename} was not same as s3"
        echo "$FUNCNAME    --Fail"
        return 1
    }
    [[ -f ${filename} ]] && {
        log_info $FUNCNAME "File ${filename} was still exist"
        echo "$FUNCNAME    --Fail"
        return 1
    }

    mkdir -p ${DIR_NAME}
    echo "${TEST_TXT}" > ${DIR_NAME}/${filename}
    message=$(mv ${DIR_NAME}/${filename} /tmp/${filename}_new 2>&1)
    if [[ $? != 0 || ! -f /tmp/${filename}_new ]]; then
        log_info $FUNCNAME "File /tmp/${filename}_new was not mv succeed, error: ${message}"
        echo "$FUNCNAME    --Fail"
        return 1
    fi
    cmp /tmp/${filename}_new  ${DIR_NAME}/${filename}
    [[ $? != 0 ]] && {
        log_info $FUNCNAME "File /tmp/${filename} was not same as s3"
        echo "$FUNCNAME    --Fail"
        return 1
    }
    [[ -f ${DIR_NAME}/${filename} ]] && {
        log_info $FUNCNAME "File ${DIR_NAME}/${filename} was still exist"
        echo "$FUNCNAME    --Fail"
        return 1
    }
    echo "$FUNCNAME    --Success"
    rm -fr /tmp/${filename}.* ${DIR_NAME} ${filename}
}

function test_16_cat_file
{
    filename="test$(cat /dev/urandom | head -c 5 | md5sum | head -c 5).txt"
    echo "${TEST_TXT}" > /tmp/${filename}
    ${aws_cli} put-object --bucket ${bucket} --key ${filename} --body /tmp/${filename} >/dev/null 2>&1
    [[ $? != 0 ]] &&{
        log_info $FUNCNAME "put ${filename} on s3 failed"
        echo "$FUNCNAME    --Fail"
        return 1
    }
    sleep 60
    if [ ! -f ${filename} ]; then
        log_info $FUNCNAME "file ${filename} was not existed on local"
        echo "$FUNCNAME    --Fail"
        return 1
    fi
    content=$(cat ${filename})
    [[ "${TEST_TXT}" != "${content}" ]] && {
        log_info $FUNCNAME "file dir1/${filename} on s3 was not same as local"
        echo "$FUNCNAME    --Fail"
        return 1
     }
    echo "$FUNCNAME    --Success"
    rm -fr ${filename} /tmp/${filename}
}

function test_17_rename_dir
{
    DIR_NAME="DIR$(cat /dev/urandom | head -c 5 | md5sum | head -c 5)"
    mkdir -p  ${DIR_NAME}
    #rename dir
    message=$(mv ${DIR_NAME} ${DIR_NAME}_new 2>&1)
    if [[ $? != 0 || ! -d ${DIR_NAME}_new ]]; then
        log_info $FUNCNAME "Directory ${DIR_NAME} rename not success, error: ${message}"
        echo "$FUNCNAME    --Fail"
        return 1
    fi
    echo "$FUNCNAME    --Success"
    rm -fr ${DIR_NAME}
}

function test_18_rename_dir_with_file
{
    DIR_NAME="DIR$(cat /dev/urandom | head -c 5 | md5sum | head -c 5)"
    mkdir -p  ${DIR_NAME}
    date > ${DIR_NAME}/${filename}
    #rename dir
    message=$(mv ${DIR_NAME} ${DIR_NAME}_new 2>&1)
    if [[ $? != 0 || ! -d ${DIR_NAME}_new ]]; then
        log_info $FUNCNAME "Directory ${DIR_NAME} rename not success, error: ${message}"
        echo "$FUNCNAME    --Fail"
        return 1
    fi
    echo "$FUNCNAME    --Success"
    rm -fr ${DIR_NAME}
}

function test_19_rename_file
{
    filename="test$(cat /dev/urandom | head -c 5 | md5sum | head -c 5).txt"
    date > ${filename}
    #rename dir
    message=$(mv ${filename} ${filename}_new 2>&1)
    if [[ $? != 0 || ! -f ${filename}_new ]]; then
        log_info $FUNCNAME "File ${filename} rename not success, error: ${message}"
        echo "$FUNCNAME    --Fail"
        return 1
    fi
    echo "$FUNCNAME    --Success"
    rm -fr ${DIR_NAME}
}

function test_20_rm_file
{
    filename="test$(cat /dev/urandom | head -c 5 | md5sum | head -c 5).txt"
    date > ${filename}
    #rename file
    message=$(rm ${filename} 2>&1)
    if [[ $? != 0 || -f ${filename}_new ]]; then
        log_info $FUNCNAME "File ${filename} delete not success, error: ${message}"
        echo "$FUNCNAME    --Fail"
        return 1
    fi
    ${aws_cli} get-object --bucket ${bucket} --key ${filename} /tmp/${filename} >/dev/null 2>&1
    [[ $? == 0 ]] &&{
        log_info $FUNCNAME "get ${filename} on s3 should be failed"
        echo "$FUNCNAME    --Fail"
        return 1
    }
    echo "$FUNCNAME    --Success"
    rm -fr ${filename}
}

function test_21_rm_dir
{
    DIR_NAME="test$(cat /dev/urandom | head -c 5 | md5sum | head -c 5).txt"
    mkdir -p  ${DIR_NAME}
    #rename dir
    message=$(rm -fr ${DIR_NAME} 2>&1)
    if [[ $? != 0 || -d ${DIR_NAME} ]]; then
        log_info $FUNCNAME "Directory ${DIR_NAME} delete not success, error: ${message}"
        echo "$FUNCNAME    --Fail"
        return 1
    fi
    ${aws_cli} list-objects --bucket ${bucket}|grep "${DIR_NAME}/" >/dev/null
    [[ $? == 0 ]] &&{
        log_info $FUNCNAME "list ${DIR_NAME} on s3 should be failed"
        echo "$FUNCNAME    --Fail"
        return 1
    }
    echo "$FUNCNAME    --Success"
    rm -fr ${DIR_NAME}
}

function test_22_rm_dir_with_file
{
    DIR_NAME="test$(cat /dev/urandom | head -c 5 | md5sum | head -c 5).txt"
    mkdir -p  ${DIR_NAME}
    date > ${DIR_NAME}/file1
    #rename dir
    message=$(rm -fr ${DIR_NAME} 2>&1)
    if [[ $? != 0 || -d ${DIR_NAME} ]]; then
        log_info $FUNCNAME "Directory ${DIR_NAME} delete not success, error: ${message}"
        echo "$FUNCNAME    --Fail"
        return 1
    fi
    ${aws_cli} list-objects --bucket ${bucket}|grep "${DIR_NAME}/file1" >/dev/null
    [[ $? == 0 ]] &&{
        log_info $FUNCNAME "get ${DIR_NAME} on s3 should be failed"
        echo "$FUNCNAME    --Fail"
        return 1
    }
    echo "$FUNCNAME    --Success"
    rm -fr ${DIR_NAME}
}


function run_all_cases
{
    test_01_mkdir
    test_02_cpdir
    test_03_mvdir
    test_04_mkdir_depth
    test_05_touch_file
    test_06_echo_file
    test_07_cp_file
    test_08_mv_file
    test_09_larger_file
    test_10_internal_cp_file
    test_11_list_object
    test_12_external_cp_dir
    test_13_external_mv_dir
    test_14_external_cp_file
    test_15_external_mv_file
    test_16_cat_file
    test_17_rename_dir
    test_18_rename_dir_with_file
    test_19_rename_file
    test_20_rm_file
    test_21_rm_dir
    test_22_rm_dir_with_file

}
echo "test dir: ${test_dir}"
echo "verify s3: ${s3url}"
echo "test bucket: ${bucket}"
cd ${test_dir}
run_all_cases