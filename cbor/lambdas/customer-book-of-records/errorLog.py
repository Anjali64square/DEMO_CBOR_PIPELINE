import iSeries  # ✅ using common DB module (jaydebeapi + MySQL)
from sendEmail import send_email


def log_error(error_payload):
    response = None
    error_log_input_params = {
        'IN_p_PayloadId': error_payload['PayLoadId'],
        'IN_p_JobControlId': error_payload['JobControlId'],
        'IN_p_ErrorType': f"'{error_payload['ErrorType']}'",
        'IN_p_ErrorMessage': f"'{error_payload['ErrorMessage']}'",
        'IN_p_UserId': "'pro_auth_user'"
    }

    print('START: creating error log')
    try:
        results = iSeries.executeProcedure(
            'insert_nonma_error_log', error_log_input_params)
        response = results
    except Exception as err:
        print('ERROR: ErrorLog Error:', err)

    if response is not None:
        if response.get('status') == 'success':
            print('INFO: successfully logged error details')
            print('END: creating error log')

    email_resp = None
    print('START: sending email notification')
    try:
        email_resp = send_email(str(error_log_input_params))
    except Exception as err:
        print('ERROR: Email Error:', err)

    if email_resp is not None and email_resp == 'success':
        print('INFO: email sent successfully.')
        print('END: sending email notification')
