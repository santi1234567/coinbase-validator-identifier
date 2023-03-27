from typing import Any, Dict, cast, Union

from eth_typing import HexStr
from eth_utils import event_abi_to_log_topic
from hexbytes import HexBytes
from web3._utils.abi import get_abi_input_names, get_abi_input_types, map_abi_data
from web3._utils.normalizers import BASE_RETURN_NORMALIZERS
from web3.contract import Contract


class EventLogDecoder:
    def __init__(self, contract: Contract):
        self.contract = contract
        self.event_abis = [
            abi for abi in self.contract.abi if abi['type'] == 'event']
        self._sign_abis = {event_abi_to_log_topic(
            abi): abi for abi in self.event_abis}
        self._name_abis = {abi['name']: abi for abi in self.event_abis}

    def decode_log(self, result: Dict[str, Any]):
        data = [t[2:] for t in result['topics']]
        data += [result['data'][2:]]
        data = "0x" + "".join(data)
        return self.decode_event_input(data)

    def decode_event_input(self, data: Union[HexStr, str], name: str = None) -> Dict[str, Any]:
        # type ignored b/c expects data arg to be HexBytes
        data = HexBytes(data)  # type: ignore
        selector, params = data[:32], data[32:]

        if name:
            func_abi = self._get_event_abi_by_name(event_name=name)
        else:
            func_abi = self._get_event_abi_by_selector(selector)

        names = get_abi_input_names(func_abi)
        types = get_abi_input_types(func_abi)

        decoded = self.contract.web3.codec.decode(
            types, cast(HexBytes, params))
        normalized = map_abi_data(BASE_RETURN_NORMALIZERS, types, decoded)

        return dict(zip(names, normalized))

    def _get_event_abi_by_selector(self, selector: HexBytes) -> Dict[str, Any]:
        try:
            return self._sign_abis[selector]
        except KeyError:
            raise ValueError("Event is not presented in contract ABI.")

    def _get_event_abi_by_name(self, event_name: str) -> Dict[str, Any]:
        try:
            return self._name_abis[event_name]
        except KeyError:
            raise KeyError(
                f"Event named '{event_name}' was not found in contract ABI.")
