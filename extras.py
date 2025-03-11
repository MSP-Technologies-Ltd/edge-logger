from datetime import datetime

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


# take data provided by main program
# use the deviceId to determine what data to process
# return a dictionary with the processed data
# when this dictionary is returned, the main program will save it to file
async def process_device_data(device, data):
    try:
        data_to_save = {}
        if "battery" in device:
            data_to_save = await process_battery(data)
        if "controller" in device:
            data_to_save = await process_controller(data, data_to_save)
        if "inverter" in device:
            data_to_save = await process_inverter(data, data_to_save)

        return data_to_save
    except Exception as e:
        print(f"Error processing data for {device}")


async def process_battery(data):
    if data is not None:
        try:
            standard_data = data.get("standard")
            unparsed_data = data.get("data")
            parsed_data = data.get("parsed data")

            timestamp = data.get("timestamp")

            if timestamp is None:
                timestamp = datetime.now().isoformat()

            return_list = list()

            standard_data = {**standard_data, type: "standard_data"}
            unparsed_data = {**unparsed_data, type: "unparsed_data"}
            parsed_data = {**parsed_data, type: "parsed_data"}

            return_list.append(standard_data, unparsed_data, parsed_data)

            return return_list

        except Exception as e:
            print(f"Error processing battery data: {e}")


async def process_controller(data):
    if data is not None:
        try:
            if "globalState" in data:
                global_state = data.get("globalState")
                return global_state
        except Exception as e:
            print(f"Error processing controller data: {e}")


async def process_inverter(data):
    if data is not None:
        try:
            if data is not None:
                pass
            pass
        except Exception as e:
            print(f"Error processing inverter data: {e}")
