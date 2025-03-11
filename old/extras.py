from datetime import datetime
import traceback

# space for methods that might take up too much space
# namely for processing certain devices data


def get_device_id(data):
    device_id = None

    device_id = data.get("deviceId")

    if device_id is None:
        device_id = data.get("device_id")

    if device_id is None:
        device_id = data.get("Unique ID")

    if device_id is None:
        device_id = data.get("client_id")

    return device_id


def get_device_type(data):
    device_type = None

    device_type = data.get("Device")

    if device_type is None:
        device_type = data.get("deviceType")

    return device_type


# take data provided by main program
# use the deviceId to determine what data to process
# return a dictionary with the processed data
# when this dictionary is returned, the main program will save it to file
async def process_device_data(device, data):
    # print(device)
    try:
        data_to_save = {}
        if "battery" in device:
            data_to_save = await process_battery(data)
        elif "controller" in device:
            # print(f"Processing controller: {device}")  # Extra logging
            data_to_save = await process_controller(data)
        elif "inverter" in device:
            data_to_save = await process_inverter(data)
        else:
            raise Exception(f"Unrecognized device type: {device}")

        return data_to_save
    except Exception as e:
        print(f"Error processing data for {device}: {e}, data: {data}")


async def process_battery(data):
    if data is not None:
        try:
            standard_data = data.get("standard")
            unparsed_data = data.get("data")
            parsed_data = data.get("parsed data")

            timestamp = data.get("timestamp")

            if timestamp is None:
                timestamp = datetime.now().isoformat()

            return_list = []  # Initialize as empty list

            if standard_data:
                standard_data = {**standard_data, "dataType": "standard_data"}
                return_list.append(standard_data)
            
            if unparsed_data:
                unparsed_data = {**unparsed_data, "dataType": "unparsed_data"}
                return_list.append(unparsed_data)
                
            if parsed_data:
                parsed_data = {**parsed_data, "dataType": "parsed_data"}
                return_list.append(parsed_data)

            return return_list

        except Exception as e:
            print(f"Error processing battery data: {e}")
            traceback.print_exc()  # Add stack trace for better debugging

async def process_controller(data):
    # print(f"Starting processing for device: {data.get('deviceId', 'Unknown')}")

    if data is not None:
        return_list = []
        try:
            # print(f"Controller data received: {data}")

            timestamp = data.get("timestamp", datetime.now().isoformat())
            # print(f"Timestamp: {timestamp}")

            global_state = data.get("globalState")
            # print(f"Global state present: {bool(global_state)}")

            if global_state:
                last_known = global_state.pop("lastKnownGood", None)
                # print(f"Last known good data present: {bool(last_known)}")

                # if last_known:
                #     last_known_data = {
                #         **last_known,
                #         "timestamp": timestamp,
                #         "dataType": "lastKnown",
                #     }
                #     return_list.append(last_known_data)

                global_state_data = {
                    **global_state,
                    "timestamp": timestamp,
                    "dataType": "global",
                }
                return_list.append(global_state_data)

                # print("Successfully processed global state")
                return return_list

            print("No global state found, returning fallback data")
            return [{"timestamp": timestamp, "dataType": "controller", **data}]

        except Exception as e:
            print(f"Unexpected error processing controller data: {e}")
            traceback.print_exc()
        except KeyError as e:
            print(f"Missing key in controller data: {e}")
        # finally:
        #     print(
        #         f"Finished processing data for device: {data.get('deviceId', 'Unknown')}"
        #     )

    else:
        print("No data provided")


async def process_inverter(data):
    try:
        pass
    except Exception as e:
        print(f"Error processing inverter data: {e}")
