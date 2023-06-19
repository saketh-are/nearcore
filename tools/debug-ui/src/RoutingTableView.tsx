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
    return (
        <table className="network-distances-table">
            <thead>
                <th>Peer ID</th>
                <th>Shortest Path Length (Hops)</th>
            </thead>
            <tbody>
                {Object.entries(routingTable!.status_response.Routes.my_distances).map( ([peer_id, distance]) => {
                    return (
                        <tr key={peer_id}>
                            <td>{peer_id}</td>
                            <td>{distance}</td>
                        </tr>
                    );
                })}
            </tbody>
        </table>
    );
};
