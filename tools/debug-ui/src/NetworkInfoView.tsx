import './NetworkInfoView.scss';
import { NavLink, Route, Routes } from 'react-router-dom';
import { CurrentPeersView } from './CurrentPeersView';
import { PeerStorageView } from './PeerStorageView';
import { ConnectionStorageView } from './ConnectionStorageView';
import { Tier1View } from './Tier1View';
import { RoutingTableView } from './RoutingTableView';
import { SnapshotHostsView } from './SnapshotHostsView';

type NetworkInfoViewProps = {
    addr: string;
};

export const NetworkInfoView = ({ addr }: NetworkInfoViewProps) => {
    return (
        <div className="network-info-view">
            <div className="navbar">
                <NavLink to="../current" className={navLinkClassName}>
                    Current Peers
                </NavLink>
                <NavLink to="../peer_storage" className={navLinkClassName}>
                    Detailed Peer Storage
                </NavLink>
                <NavLink to="../connection_storage" className={navLinkClassName}>
                    Connection Storage
                </NavLink>
                <NavLink to="../tier1" className={navLinkClassName}>
                    TIER1
                </NavLink>
                <NavLink to="../routing_table" className={navLinkClassName}>
                    Routing Table
                </NavLink>
                <NavLink to="../snapshot_hosts" className={navLinkClassName}>
                    Snapshot Hosts
                </NavLink>
            </div>
            <Routes>
                <Route path="current" element={<CurrentPeersView addr={addr} />} />
                <Route path="peer_storage" element={<PeerStorageView addr={addr} />} />
                <Route path="connection_storage" element={<ConnectionStorageView addr={addr} />} />
                <Route path="tier1" element={<Tier1View addr={addr} />} />
                <Route path="routing_table" element={<RoutingTableView addr={addr} />} />
                <Route path="snapshot_hosts" element={<SnapshotHostsView addr={addr} />} />
            </Routes>
        </div>
    );
};

function navLinkClassName({ isActive }: { isActive: boolean }) {
    return isActive ? 'nav-link active' : 'nav-link';
}
