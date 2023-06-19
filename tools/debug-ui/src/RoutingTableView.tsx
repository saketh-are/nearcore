import { useQuery } from 'react-query';
import { toHumanTime } from './utils';
import { fetchRoutingTable } from './api';
import './RoutingTableView.scss';

type RoutingTableViewProps = {
    addr: string;
};

export const RoutingTableView = ({ addr }: RoutingTableViewProps) => {
    const {
        data: routingTable,
        error,
        isLoading,
    } = useQuery(['routingTable', addr], () => fetchRoutingTable(addr));

    if (isLoading) {
        return <div>Loading...</div>;
    } else if (error) {
        return <div className="error">{(error as Error).stack}</div>;
    }

    const routingInfo = routingTable!.status_response.Routes;
    const peerLabels = routingInfo.edge_cache.peer_labels;

    const peers = Object.entries(routingInfo.my_distances).map( ([peer_id, distance]) => {
        return [peer_id, peerLabels[peer_id], distance];
    });
    peers.sort((a, b) => a[1] > b[1] ? 1 : -1);

    return (
        <div className="routing-table-view">
            <p><b>Routable Peers</b></p>
            <table>
                <thead>
                    <th>Peer ID</th>
                    <th>Peer Label</th>
                    <th>Shortest Path Length (Hops)</th>
                </thead>
                <tbody>
                    {peers.map(([peer_id, peer_label, distance]) => {
                        return (
                            <tr key={peer_label}>
                                <td>{peer_id}</td>
                                <td>{peer_label}</td>
                                <td>{distance}</td>
                            </tr>
                        );
                    })}
                </tbody>
            </table>
        </div>
    );
};
