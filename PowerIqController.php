<?php

namespace Power\Controller\Script;

use Zend\Mvc\Controller\AbstractActionController;
use Zend\Log\Logger;
use Zend\Db\Sql\Expression;
use Zend\View\Model\ConsoleModel;
use Zend\Server\Reflection;
use Zend\Json\Json;
use Zend\Http\Client;
use Zend\Console\Exception\InvalidArgumentException;

use Gem\Model\AssetRacks;
use Gem\Model\AssetDevices;
use Gem\Model\AssetDeviceIps;


use Power\Model\PowerPdus;
use Power\Model\PowerRacks;
use Power\Model\PowerAisles;
use Power\Model\PowerFloors;
use Power\Model\PowerRooms;
use Power\Model\PowerDatacenters;

class PowerIqController extends AbstractActionController
{
    private $client;

    const ALL = 63;
    const DATACENTER = 1;
    const FLOOR = 2;
    const ROOM = 4;
    const AISLE = 8;
    const RACK = 16;
    const PDU = 32;

    public function importAction()
    {
        $sm = $this->getServiceLocator();

        $db = $sm->get('db');
        $config = $sm->get('Config');
        $logger = $sm->get('console_logger');
        $result = new ConsoleModel();

        $type = $this->params()->fromRoute('type');
        if (empty($type)) {
            $type = 'ALL';
        } else {
            $type = strtoupper($type);
        }

        if (!in_array($type, array('DATACENTER', 'FLOOR', 'ROOM', 'AISLE', 'RACK', 'PDU', 'ALL'))) {
            throw new InvalidArgumentException("invalid type, please use (Datacenter, Floor, Room, Aisle, Rack, Pdu, or All)");
        }

        //$rack_id = $this->powerIqRackToGemRack(2431);
        //var_dump($rack_id);
        //exit;

        $logger->log(Logger::INFO, "starting power-iq import");

        $cls = new \ReflectionClass(__CLASS__);
        $type = $cls->getConstant($type);

        try {

            if (($type & self::DATACENTER) == self::DATACENTER) {
                //$this->processDataCenters();
            }

            if (($type & self::FLOOR) == self::FLOOR) {
                //$this->processFloors();
            }

            if (($type & self::ROOM) == self::ROOM) {
                //$this->processRooms();
            }

            if (($type & self::AISLE) == self::AISLE) {
                //$this->processAisles();
            }

            if (($type & self::RACK) == self::RACK) {
                //$this->processRacks();
            }
// I just want to log these lines of code, they exist in the pdu variable
            if (($type & self::PDU) == self::PDU) {
                $this->processAvocentPdus();
                $this->processEmersonPdus();
                $this->processRaritanPdus();
                $this->processEatonPdus();

            }

            $logger->log(Logger::INFO, "finished power-iq import");
        } catch (\Exception $e) {
            $logger->log(Logger::ERR, "unable to import power-iq : " . $e->getMessage());
            $logger->log(Logger::ERR, $e->getTraceAsString());
            $result->setErrorLevel(1);
        }

        return $result;
    }

    protected function buildClient()
    {
        if (!empty($this->client)) {
            return $this->client;
        }

        $sm = $this->getServiceLocator();
        $config = $sm->get('Config');
        $logger = $sm->get('console_logger');

        if (empty($config['power-iq'])) {
            $logger->log(Logger::ERR, "No power-iq configurations");
            return false;
        }

        if (empty($config['power-iq']['host'])) {
            $logger->log(Logger::ERR, "No power-iq hostname");
            return false;
        }

        if (empty($config['power-iq']['username'])) {
            $logger->log(Logger::ERR, "No power-iq username");
            return false;
        }

        $this->client = new Client("https://{$config['power-iq']['host']}", array(
            'sslverifypeer' => false,
            'sslallowselfsigned' => true
        ));
        $this->client->setAuth($config['power-iq']['username'], $config['power-iq']['password']);
        $headers = $this->client->getRequest()->getHeaders();
        // required for a successfull api call
        $headers->addHeaderLine('Content-Type: application/json');
        $headers->addHeaderLine('Accept: application/json; charset=utf-8');

        return $this->client;
    }

    protected function processDataCenters()
    {
        $sm = $this->getServiceLocator();
        $config = $sm->get('Config');
        $db = $sm->get('db');
        $logger = $sm->get('console_logger');

        $power_datacenters_mdl = PowerDatacenters::factory($sm);

        $batch = 0;
        $batchsize = 50;
        $last_id = null;
        $more = false;

        $con = $db->getDriver()->getConnection();

        $logger->log(Logger::INFO, "starting power-iq processing on datacenters");

        do {
            $more = false;
            $client = $this->buildClient();
            if ($client === false) {
                throw new \Exception("unable to establish client");
            }

            $url = "https://{$config['power-iq']['host']}/api/v2/data_centers?limit={$batchsize}&order=id.asc";
            if ($last_id) {
                $url .= "&id_gt={$last_id}";
            }

            try {
                $logger->log(Logger::INFO, "processing power-iq datacenters batch {$batch}");
                $logger->log(Logger::INFO, "url: {$url}");

                $client->setUri($url);
                $response = $client->send();
                if ($response->isSuccess()) {
                    $body = $response->getBody();
                    $data = Json::decode($body, Json::TYPE_ARRAY);

                    if (!empty($data['data_centers'])) {
                        try {
                            $con->beginTransaction();

                            foreach ($data['data_centers'] as $datacenter) {
                                $row = $power_datacenters_mdl->get(array(
                                    'power_iq_datacenter_id' => $datacenter['id']
                                ));

                                if ($row === false) {
                                    // insert this datacenter
                                    $id = $power_datacenters_mdl->insert(
                                        $datacenter['id'],
                                        $datacenter['name'],
                                        $datacenter['city'],
                                        $datacenter['state'],
                                        $datacenter['country']
                                    );
                                } else {
                                    // update this datacenter
                                    $power_datacenters_mdl->update($row->id, array(
                                        'name' => $datacenter['name'],
                                        'city' => $datacenter['city'],
                                        'state' => $datacenter['state'],
                                        'country' => $datacenter['country']
                                    ));
                                }

                                $last_id = $datacenter['id'];
                                $more = true;
                            }

                            $con->commit();
                        } catch (\Exception $e) {
                            $con->rollback();
                            $p = $e->getPrevious();
                            if ($p) {
                                $e = $p;
                            }

                            $logger->log(Logger::ERR, "unable to finish importing power-iq datacenters on batch {$batch} : " . $e->getMessage());
                            break;
                        }
                    } else {
                        // no more datacenter
                        $logger->log(Logger::INFO, "reached end of all datacenters");
                        break;
                    }
                } else {
                    $logger->log(Logger::ERR, "unable to request for power-iq datacenters : " . $response->getStatusCode());
                    $logger->log(Logger::ERR, $response->getBody());
                    break;
                }
            } catch (\Exception $e) {
                $logger->log(Logger::ERR, "unable to finish importing power-iq datacenters on batch {$batch} : " . $e->getMessage());
                $logger->log(Logger::ERR, $e->getTraceAsString());
                break;
            }

            $batch++;
        } while ($more);

        $logger->log(Logger::INFO, "finished power-iq processing on datacenters");
    }

    protected function processFloors()
    {
        $sm = $this->getServiceLocator();
        $config = $sm->get('Config');
        $db = $sm->get('db');
        $logger = $sm->get('console_logger');

        $power_floors_mdl = PowerFloors::factory($sm);

        $batch = 0;
        $batchsize = 50;
        $last_id = null;
        $more = false;

        $con = $db->getDriver()->getConnection();

        $logger->log(Logger::INFO, "starting power-iq processing on floors");

        do {
            $more = false;
            $client = $this->buildClient();
            if ($client === false) {
                throw new \Exception("unable to establish client");
            }

            $url = "https://{$config['power-iq']['host']}/api/v2/floors?limit={$batchsize}&order=id.asc";
            if ($last_id) {
                $url .= "&id_gt={$last_id}";
            }

            try {
                $logger->log(Logger::INFO, "processing power-iq floors batch {$batch}");
                $logger->log(Logger::INFO, "url: {$url}");

                $client->setUri($url);
                $response = $client->send();
                if ($response->isSuccess()) {
                    $body = $response->getBody();
                    $data = Json::decode($body, Json::TYPE_ARRAY);

                    if (!empty($data['floors'])) {
                        try {
                            $con->beginTransaction();

                            foreach ($data['floors'] as $floor) {
                                $row = $power_floors_mdl->get(array(
                                    'power_iq_floor_id' => $floor['id']
                                ));

                                $datacenter_id = null;
                                if (!empty($floor['parent']) && !empty($floor['parent']['id'])) {
                                    if ($floor['parent']['type'] == 'data_center') {
                                        $datacenter_id = $floor['parent']['id'];
                                    }
                                }

                                if ($row === false) {
                                    // insert this floor
                                    $id = $power_floors_mdl->insert(
                                        $floor['id'],
                                        $floor['name'],
                                        $datacenter_id
                                    );
                                } else {
                                    // update this floor
                                    $power_floors_mdl->update($row->id, array(
                                        'name' => $floor['name'],
                                        'power_iq_datacenter_id' => $datacenter_id
                                    ));
                                }

                                $last_id = $floor['id'];
                                $more = true;
                            }

                            $con->commit();
                        } catch (\Exception $e) {
                            $con->rollback();
                            $p = $e->getPrevious();
                            if ($p) {
                                $e = $p;
                            }

                            $logger->log(Logger::ERR, "unable to finish importing power-iq floors on batch {$batch} : " . $e->getMessage());
                            break;
                        }
                    } else {
                        // no more floor
                        $logger->log(Logger::INFO, "reached end of all floors");
                        break;
                    }
                } else {
                    $logger->log(Logger::ERR, "unable to request for power-iq floors : " . $response->getStatusCode());
                    $logger->log(Logger::ERR, $response->getBody());
                    break;
                }
            } catch (\Exception $e) {
                $logger->log(Logger::ERR, "unable to finish importing power-iq floors on batch {$batch} : " . $e->getMessage());
                $logger->log(Logger::ERR, $e->getTraceAsString());
                break;
            }

            $batch++;
        } while ($more);

        $logger->log(Logger::INFO, "finished power-iq processing on floors");
    }

    protected function processRooms()
    {
        $sm = $this->getServiceLocator();
        $config = $sm->get('Config');
        $db = $sm->get('db');
        $logger = $sm->get('console_logger');

        $power_rooms_mdl = PowerRooms::factory($sm);

        $batch = 0;
        $batchsize = 50;
        $last_id = null;
        $more = false;

        $con = $db->getDriver()->getConnection();

        $logger->log(Logger::INFO, "starting power-iq processing on rooms");

        do {
            $more = false;
            $client = $this->buildClient();
            if ($client === false) {
                throw new \Exception("unable to establish client");
            }

            $url = "https://{$config['power-iq']['host']}/api/v2/rooms?limit={$batchsize}&order=id.asc";
            if ($last_id) {
                $url .= "&id_gt={$last_id}";
            }

            try {
                $logger->log(Logger::INFO, "processing power-iq rooms batch {$batch}");
                $logger->log(Logger::INFO, "url: {$url}");

                $client->setUri($url);
                $response = $client->send();
                if ($response->isSuccess()) {
                    $body = $response->getBody();
                    $data = Json::decode($body, Json::TYPE_ARRAY);

                    if (!empty($data['rooms'])) {
                        try {
                            $con->beginTransaction();

                            foreach ($data['rooms'] as $room) {
                                $row = $power_rooms_mdl->get(array(
                                    'power_iq_room_id' => $room['id']
                                ));

                                $datacenter_id = null;
                                $floor_id = null;
                                if (!empty($room['parent']) && !empty($room['parent']['id'])) {
                                    if ($room['parent']['type'] == 'data_center') {
                                        $datacenter_id = $room['parent']['id'];
                                    } else if ($room['parent']['type'] == 'floor') {
                                        $floor_id = $room['parent']['id'];
                                    }
                                }

                                if ($row === false) {
                                    // insert this room
                                    $id = $power_rooms_mdl->insert(
                                        $room['id'],
                                        $room['name'],
                                        $datacenter_id,
                                        $floor_id
                                    );
                                } else {
                                    // update this room
                                    $power_rooms_mdl->update($row->id, array(
                                        'name' => $room['name'],
                                        'power_iq_datacenter_id' => $datacenter_id,
                                        'power_iq_floor_id' => $floor_id
                                    ));
                                }

                                $last_id = $room['id'];
                                $more = true;
                            }

                            $con->commit();
                        } catch (\Exception $e) {
                            $con->rollback();
                            $p = $e->getPrevious();
                            if ($p) {
                                $e = $p;
                            }

                            $logger->log(Logger::ERR, "unable to finish importing power-iq rooms on batch {$batch} : " . $e->getMessage());
                            break;
                        }
                    } else {
                        // no more room
                        $logger->log(Logger::INFO, "reached end of all rooms");
                        break;
                    }
                } else {
                    $logger->log(Logger::ERR, "unable to request for power-iq rooms : " . $response->getStatusCode());
                    $logger->log(Logger::ERR, $response->getBody());
                    break;
                }
            } catch (\Exception $e) {
                $logger->log(Logger::ERR, "unable to finish importing power-iq rooms on batch {$batch} : " . $e->getMessage());
                $logger->log(Logger::ERR, $e->getTraceAsString());
                break;
            }

            $batch++;
        } while ($more);

        $logger->log(Logger::INFO, "finished power-iq processing on rooms");
    }

    protected function processAisles()
    {
        $sm = $this->getServiceLocator();
        $config = $sm->get('Config');
        $db = $sm->get('db');
        $logger = $sm->get('console_logger');

        $power_aisles_mdl = PowerAisles::factory($sm);

        $batch = 0;
        $batchsize = 50;
        $last_id = null;
        $more = false;

        $con = $db->getDriver()->getConnection();

        $logger->log(Logger::INFO, "starting power-iq processing on aisles");

        do {
            $more = false;
            $client = $this->buildClient();
            if ($client === false) {
                throw new \Exception("unable to establish client");
            }

            $url = "https://{$config['power-iq']['host']}/api/v2/aisles?limit={$batchsize}&order=id.asc";
            if ($last_id) {
                $url .= "&id_gt={$last_id}";
            }

            try {
                $logger->log(Logger::INFO, "processing power-iq aisles batch {$batch}");
                $logger->log(Logger::INFO, "url: {$url}");

                $client->setUri($url);
                $response = $client->send();
                if ($response->isSuccess()) {
                    $body = $response->getBody();
                    $data = Json::decode($body, Json::TYPE_ARRAY);

                    if (!empty($data['aisles'])) {
                        try {
                            $con->beginTransaction();

                            foreach ($data['aisles'] as $aisle) {
                                $row = $power_aisles_mdl->get(array(
                                    'power_iq_aisle_id' => $aisle['id']
                                ));

                                $floor_id = null;
                                $room_id = null;
                                if (!empty($aisle['parent']) && !empty($aisle['parent']['id'])) {
                                    if ($aisle['parent']['type'] == 'floor') {
                                        $floor_id = $aisle['parent']['id'];
                                    } else if ($aisle['parent']['type'] == 'room') {
                                        $room_id = $aisle['parent']['id'];
                                    }
                                }

                                if ($row === false) {
                                    // insert this aisle
                                    $id = $power_aisles_mdl->insert(
                                        $aisle['id'],
                                        $aisle['name'],
                                        $floor_id,
                                        $room_id
                                    );
                                } else {
                                    // update this aisle
                                    $power_aisles_mdl->update($row->id, array(
                                        'name' => $aisle['name'],
                                        'power_iq_floor_id' => $floor_id,
                                        'power_iq_room_id' => $room_id
                                    ));
                                }

                                $last_id = $aisle['id'];
                                $more = true;
                            }

                            $con->commit();
                        } catch (\Exception $e) {
                            $con->rollback();
                            $p = $e->getPrevious();
                            if ($p) {
                                $e = $p;
                            }

                            $logger->log(Logger::ERR, "unable to finish importing power-iq aisles on batch {$batch} : " . $e->getMessage());
                            break;
                        }
                    } else {
                        // no more floor
                        $logger->log(Logger::INFO, "reached end of all aisles");
                        break;
                    }
                } else {
                    $logger->log(Logger::ERR, "unable to request for power-iq aisles : " . $response->getStatusCode());
                    $logger->log(Logger::ERR, $response->getBody());
                    break;
                }
            } catch (\Exception $e) {
                $logger->log(Logger::ERR, "unable to finish importing power-iq aisles on batch {$batch} : " . $e->getMessage());
                $logger->log(Logger::ERR, $e->getTraceAsString());
                break;
            }

            $batch++;
        } while ($more);

        $logger->log(Logger::INFO, "finished power-iq processing on aisles");
    }

    protected function processRacks()
    {
        $sm = $this->getServiceLocator();
        $config = $sm->get('Config');
        $db = $sm->get('db');
        $logger = $sm->get('console_logger');

        $power_racks_mdl = PowerRacks::factory($sm);

        $batch = 0;
        $batchsize = 100;
        $last_id = null;
        $more = false;

        $con = $db->getDriver()->getConnection();

        $logger->log(Logger::INFO, "starting power-iq processing on racks");

        do {
            $more = false;
            $client = $this->buildClient();
            if ($client === false) {
                throw new \Exception("unable to establish client");
            }

            $url = "https://{$config['power-iq']['host']}/api/v2/racks?limit={$batchsize}&order=id.asc";
            if ($last_id) {
                $url .= "&id_gt={$last_id}";
            }

            try {
                $logger->log(Logger::INFO, "processing power-iq racks batch {$batch}");
                $logger->log(Logger::INFO, "url: {$url}");

                $client->setUri($url);
                $response = $client->send();
                if ($response->isSuccess()) {
                    $body = $response->getBody();
                    $data = Json::decode($body, Json::TYPE_ARRAY);

                    if (!empty($data['racks'])) {
                        try {
                            $con->beginTransaction();

                            foreach ($data['racks'] as $rack) {
                                $row = $power_racks_mdl->get(array(
                                    'power_iq_rack_id' => $rack['id']
                                ));

                                $aisle_id = null;
                                $room_id = null;
                                if (!empty($rack['parent']) && !empty($rack['parent']['id'])) {
                                    if ($rack['parent']['type'] == 'aisle') {
                                        $aisle_id = $rack['parent']['id'];
                                    } else if ($rack['parent']['type'] == 'room') {
                                        $room_id = $rack['parent']['id'];
                                    }
                                }

                                if ($row === false) {
                                    // insert this rack
                                    $id = $power_racks_mdl->insert(
                                        $rack['id'],
                                        new Expression('NULL'),
                                        $rack['name'],
                                        $aisle_id,
                                        $room_id
                                    );

                                    // resolve the rack name to a GEM rack id
                                    $rack_id = $this->powerIqRackToGemRack($rack['id']);
                                    if ($rack_id === false) {
                                        $rack_id = new Expression('NULL');
                                        $logger->log(Logger::WARN, "unable to resolve a GEM rack for power iq rack id {$rack['id']}");
                                    }

                                    if (!empty($rack_id)) {
                                        $power_racks_mdl->update($id, array(
                                            'rack_id' => $rack_id
                                        ));
                                    }
                                } else {
                                    // resolve the rack name to a GEM rack id
                                    $rack_id = $this->powerIqRackToGemRack($rack['id']);
                                    if ($rack_id === false) {
                                        $rack_id = new Expression('NULL');
                                        $logger->log(Logger::WARN, "unable to resolve a GEM rack for power iq rack id {$rack['id']}");
                                    }

                                    // update this rack
                                    $power_racks_mdl->update($row->id, array(
                                        'rack_id' => $rack_id,
                                        'name' => $rack['name'],
                                        'power_iq_aisle_id' => $aisle_id,
                                        'power_iq_room_id' => $room_id
                                    ));
                                }

                                $last_id = $rack['id'];
                                $more = true;
                            }

                            $con->commit();
                        } catch (\Exception $e) {
                            $con->rollback();
                            $p = $e->getPrevious();
                            if ($p) {
                                $e = $p;
                            }

                            $logger->log(Logger::ERR, "unable to finish importing power-iq racks on batch {$batch} : " . $e->getMessage());
                            break;
                        }
                    } else {
                        // no more floor
                        $logger->log(Logger::INFO, "reached end of all racks");
                        break;
                    }
                } else {
                    $logger->log(Logger::ERR, "unable to request for power-iq racks : " . $response->getStatusCode());
                    $logger->log(Logger::ERR, $response->getBody());
                    break;
                }
            } catch (\Exception $e) {
                $logger->log(Logger::ERR, "unable to finish importing power-iq racks on batch {$batch} : " . $e->getMessage());
                $logger->log(Logger::ERR, $e->getTraceAsString());
                break;
            }

            $batch++;
        } while ($more);

        $logger->log(Logger::INFO, "finished power-iq processing on racks");
    }

    protected function processAvocentPdus()
    {
        $sm = $this->getServiceLocator();
        $config = $sm->get('Config');
        $db = $sm->get('db');
        $logger = $sm->get('console_logger');

        $power_pdus_mdl = PowerPdus::factory($sm);
        $asset_devices_mdl = AssetDevices::factory($sm);
        $asset_device_ips_mdl = AssetDeviceIps::factory($sm);

        $batch = 0;
        $batchsize = 50;
        $last_id = null;
        $more = false;

        $con = $db->getDriver()->getConnection();


        $pdus = $asset_devices_mdl->filter(
            array(
                'type' => 'pdu',
                'manufacturer' => 'Avocent',
                'model' => 'PM3000'
            ),
            array(),
            array(),
            $pdu_total
        )->toArray();

        $logger->log(Logger::INFO, "processing power-iq pdus " . sizeof($pdus));
        foreach ($pdus as $pdu) {
            $connectivity = 0;

            $ips = $asset_device_ips_mdl->filter(
                array(
                    'device_id' => $pdu['id']
                ),
                array(),
                array(),
                $ip_total
            )->toArray();

            // Read values from Power Bars
            if (!empty($ips)) {
                // IP, password, oid (key)
                $current = snmpget($ips[0]['ip'], "chang3m3", ".1.3.6.1.4.1.10418.17.2.5.3.1.50.1.1");
                $logger->log(Logger::INFO, $pdu['lab'] . ' -> ' .  $pdu['rack'] . ' -> ' . $ips[0]['ip'] . ' underway!');
                if ($current) {
                    $current = (float) ltrim($current, 'INTEGER: ');
                    $current /= 10;
                    $logger->log(Logger::INFO, 'current - ' . gettype($current) . ': ' . $current);
                }

                $voltage = snmpget($ips[0]['ip'], "chang3m3", ".1.3.6.1.4.1.10418.17.2.5.3.1.70.1.1");
                if ($voltage) {
                    $voltage = (float) ltrim($voltage, 'INTEGER: ');
                    $logger->log(Logger::INFO, 'voltage - ' . gettype($voltage) . ': ' . $voltage);
                }

                $power = 0;
                $power_banks = array(
                    snmpget($ips[0]['ip'], "chang3m3", ".1.3.6.1.4.1.10418.17.2.5.11.1.60.1.1.1"),
                    snmpget($ips[0]['ip'], "chang3m3", ".1.3.6.1.4.1.10418.17.2.5.11.1.60.1.1.2"),
                    snmpget($ips[0]['ip'], "chang3m3", ".1.3.6.1.4.1.10418.17.2.5.11.1.60.1.1.3")
                );

                // Calculate integer values of each power bank
                foreach ($power_banks as $index => $power_bank) {
                    if ($power_bank) {
                        $power_banks[$index] = (float) ltrim($power_bank, 'INTEGER: ');
                        $power += $power_banks[$index] / 10;
                    }
                }

                if ($current || $voltage || $power > 0) {
                    $connectivity = 1;
                }
                $logger->log(Logger::INFO, 'power - ' . gettype($power) . ': ' . $power);

                $power_pdu = $power_pdus_mdl->get(array(
                    'power_iq_pdu_id' => $pdu['id']
                ));

                try {
                    $con->beginTransaction();

                    if ($power_pdu === false) {
                        $power_pdus_mdl->insert(
                            $pdu['id'], //power_iq_pdu_id
                            $ips[0]['ip'], //ip
                            $pdu['name'], // hostname
                            'Avocent', // manufacturer
                            'PM3000/24/30A', //model
                            'SINGLE_PHASE', //phase
                            'rack_pdu', //type
                            $pdu['rack_id'], //power_iq_rack_id
                            $pdu['rack_id'], //rack_id
                            (float) $voltage, //voltage
                            (float) $current, //current
                            (float) $power, //power
                            $connectivity, //connectivity
                            new Expression('NOW()') //rtime
                        );
                    } else {
                        $power_pdus_mdl->update($power_pdu->id, array(
                            'power_iq_pdu_id' => $pdu['id'],
                            'ip' => $ips[0]['ip'], //ip
                            'hostname' => $pdu['name'],
                            'manufacturer' => 'Avocent', // manufacturer
                            'model' => 'PM3000/24/30A', //model
                            'phase' => 'SINGLE_PHASE', //phase
                            'type' => 'rack_pdu', //type
                            'power_iq_rack_id' => $pdu['rack_id'], //power_iq_rack_id
                            'rack_id' => $pdu['rack_id'], //rack_id
                            'volt' => (float) $voltage,
                            'current' => (float) $current,
                            'power' => (float) $power,
                            'connectivity' => (float) $connectivity,
                            'rtime' =>  new Expression('NOW()') //rtime
                        ));
                    }
                    $logger->log(Logger::INFO, 'current: ' . $current);
                    $logger->log(Logger::INFO, 'power: ' . $power);

                    $con->commit();
                } catch (\Exception $e) {
                    $con->rollback();
                    /*$p = $e->getPrevious();
                    if($p) {
                        $e = $p;
                    }*/
                    $logger->log(Logger::ERR, "unable to finish importing power-iq pdus on batch {$batch} : " . $e->getMessage());
                    $logger->log(Logger::ERR, $e->getTraceAsString());
                }
            }
        }
    }

    protected function processEmersonPdus()
    {
        $sm = $this->getServiceLocator();
        $config = $sm->get('Config');
        $db = $sm->get('db');
        $logger = $sm->get('console_logger');

        $power_pdus_mdl = PowerPdus::factory($sm);
        $asset_devices_mdl = AssetDevices::factory($sm);
        $asset_device_ips_mdl = AssetDeviceIps::factory($sm);

        $batch = 0;
        $batchsize = 50;
        $last_id = null;
        $more = false;

        $con = $db->getDriver()->getConnection();


        $pdus = $asset_devices_mdl->filter(
            array(
                'type' => 'pdu',
                'manufacturer' => 'Emerson',
                'model' => 'MPH2'

            ),
            array(),
            array(),
            $pdu_total
        )->toArray();

        $logger->log(Logger::INFO, "processing power-iq pdus " . sizeof($pdus));
        foreach ($pdus as $pdu) {
            $connectivity = 0;

            $ips = $asset_device_ips_mdl->filter(
                array(
                    'device_id' => $pdu['id']
                ),
                array(),
                array(),
                $ip_total
            )->toArray();

            // Read values from Power Bars
            if (!empty($ips)) {
                // IP, password, oid (key)
                $current = snmpget($ips[0]['ip'], "ENPRackPDU_RO", ".1.3.6.1.4.1.476.1.42.3.8.30.40.1.22.1.1.1");
                $logger->log(Logger::INFO, $pdu['lab'] . ' -> ' .  $pdu['rack'] . ' -> ' . $ips[0]['ip'] . ' underway!');
                if ($current) {
                    $current = (float) ltrim($current, "Gauge32: ");
                    $current /= 100;
                    $logger->log(Logger::INFO, 'current - ' . gettype($current) . ': ' . $current);
                }

                $voltage = snmpget($ips[0]['ip'], "ENPRackPDU_RO", ".1.3.6.1.4.1.476.1.42.3.8.30.40.1.60.1.1.1");
                if ($voltage) {
                    $voltage = (float) ltrim($voltage, 'Gauge32:');
                    $logger->log(Logger::INFO, 'voltage - ' . gettype($voltage) . ': ' . $voltage);
                }

                $power = snmpget($ips[0]['ip'], "ENPRackPDU_RO", ".1.3.6.1.4.1.476.1.42.3.8.30.20.1.65.1.1");
                if ($power) {
                    $power = (float) ltrim($power, 'Gauge32:');
                    $logger->log(Logger::INFO, 'power - ' . gettype($power) . ': ' . $power);
                }

                if ($current || $voltage || $power > 0) {
                    $connectivity = 1;
                }
                // $logger->log(Logger::INFO, 'power - ' . gettype($power) . ': ' . $power);

                $power_pdu = $power_pdus_mdl->get(array(
                    'power_iq_pdu_id' => $pdu['id']
                ));

                try {
                    $con->beginTransaction();

                    if ($power_pdu === false) {
                        $power_pdus_mdl->insert(
                            $pdu['id'], //power_iq_pdu_id
                            $ips[0]['ip'], //ip
                            $pdu['name'], // hostname
                            'Emerson', // manufacturer
                            'MPH2', //model
                            'SINGLE_PHASE', //phase
                            'rack_pdu', //type
                            $pdu['rack_id'], //power_iq_rack_id
                            $pdu['rack_id'], //rack_id
                            (float) $voltage, //voltage
                            (float) $current, //current
                            (float) $power, //power
                            $connectivity, //connectivity
                            new Expression('NOW()') //rtime
                        );
                    } else {
                        $power_pdus_mdl->update($power_pdu->id, array(
                            'power_iq_pdu_id' => $pdu['id'],
                            'ip' => $ips[0]['ip'], //ip
                            'hostname' => $pdu['name'],
                            'manufacturer' => 'Emerson', // manufacturer
                            'model' => 'MPH2', //model
                            'phase' => 'SINGLE_PHASE', //phase
                            'type' => 'rack_pdu', //type
                            'power_iq_rack_id' => $pdu['rack_id'], //power_iq_rack_id
                            'rack_id' => $pdu['rack_id'], //rack_id
                            'volt' => (float) $voltage,
                            'current' => (float) $current,
                            'power' => (float) $power,
                            'connectivity' => (float) $connectivity,
                            'rtime' =>  new Expression('NOW()') //rtime
                        ));
                    }
                    $logger->log(Logger::INFO, 'current: ' . $current);
                    $logger->log(Logger::INFO, 'power: ' . $power);

                    $con->commit();
                } catch (\Exception $e) {
                    $con->rollback();
                    //  $p = $e->getPrevious();
                    //if($p) {
                    //  $e = $p;
                    //}
                    $logger->log(Logger::ERR, "unable to finish importing power-iq pdus on batch {$batch} : " . $e->getMessage());
                    $logger->log(Logger::ERR, $e->getTraceAsString());
                }
            }
        }
    }

    protected function processRaritanPdus() {
        $sm = $this->getServiceLocator();
        $config = $sm->get('Config');
        $db = $sm->get('db');
        $logger = $sm->get('console_logger');

        $power_pdus_mdl = PowerPdus::factory($sm);
        $asset_devices_mdl = AssetDevices::factory($sm);
        $asset_device_ips_mdl = AssetDeviceIps::factory($sm);

        $batch = 0;
        $batchsize = 50;
        $last_id = null;
        $more = false;

        $con = $db->getDriver()->getConnection();


        $pdus = $asset_devices_mdl->filter(
            array(
                'type' => 'pdu',
                'manufacturer' => 'Raritan',
                'retired' => 0,
                'model' => 'PX3-5497V'
            ),
            array(),
            array(),
            $pdu_total)->toArray();

	      $pdus2 = $asset_devices_mdl->filter(
            array(
                'type' => 'pdu',
                'manufacturer' => 'Raritan',
                'retired' => 0,
                'model' => 'PX3-5493V'
            ),
            array(),
            array(),
            $pdu_total)->toArray();

	      $pdus = array_merge($pdus, $pdus2);

	      $logger->log(Logger::INFO, "processing power-iq pdus " . sizeof($pdus));
        foreach($pdus as $pdu) {
            $connectivity = 0;

            $ips = $asset_device_ips_mdl->filter(
                    array(
                        'device_id' => $pdu['id']),
                    array(),
                    array(),
                    $ip_total)->toArray();

            // Read values from Power Bars
            if (!empty($ips)) {
                // IP, password, oid (key)
                $current = snmpget($ips[0]['ip'], "public", ".1.3.6.1.4.1.13742.6.5.2.3.1.6.1.1.1");
                $logger->log(Logger::INFO, $pdu['lab'] . ' -> '.  $pdu['rack'] . ' -> ' . $ips[0]['ip'] . ' underway!');
                if ($current) {
                    $current = (float)ltrim($current, 'INTEGER: ');
                    $current /= 1000;
                    $logger->log(Logger::INFO, 'current - ' . gettype($current) . ': ' . $current);
                }

                $voltage = snmpget($ips[0]['ip'], "public", ".1.3.6.1.4.1.13742.6.5.2.3.1.6.1.1.4");
                if ($voltage) {
                    $voltage = (float)ltrim($voltage, 'INTEGER: ');
                    $logger->log(Logger::INFO, 'voltage - ' . gettype($voltage) . ': ' . $voltage);
                }

                $power = snmpget($ips[0]['ip'], "public", ".1.3.6.1.4.1.13742.6.5.2.3.1.4.1.1.5");
		            if ($power) {
                    #$logger->log(Logger::INFO, 'power - ' . gettype($power) . ': ' . $power);
                    $power = (float)ltrim($power, 'Gauge32:');
                    $logger->log(Logger::INFO, 'power - ' . gettype($power) . ': ' . $power);
		            }

                if ($current || $voltage || $power > 0) {
                    $connectivity = 1;
                }
                #$logger->log(Logger::INFO, 'power - ' . gettype($power) . ': ' . $power);

                $power_pdu = $power_pdus_mdl->get(array(
                    'power_iq_pdu_id' => $pdu['id']
                ));





		          try {
                    $con->beginTransaction();

                    if ($power_pdu === false) {
                        $power_pdus_mdl->insert(
                            $pdu['id'],//power_iq_pdu_id
                            $ips[0]['ip'],//ip
                            $pdu['name'],// hostname
                            'Raritan', // manufacturer
                            'PX3-5497V/24/30A',//model
                            'SINGLE_PHASE',//phase
                            'rack_pdu',//type
                            $pdu['rack_id'],//power_iq_rack_id
                            $pdu['rack_id'],//rack_id
                            (float)$voltage,//voltage
                            (float)$current,//current
                            (float)$power,//power
                            $connectivity,//connectivity
                            new Expression('NOW()') //rtime
                        );
                    } else {
                        $power_pdus_mdl->update($power_pdu->id, array(
                            'power_iq_pdu_id' => $pdu['id'],
                            'ip' => $ips[0]['ip'],//ip
                            'hostname' => $pdu['name'],
                            'manufacturer' => 'Raritan', // manufacturer
                            'model' => 'PX3-5497V/24/30A',//model
                            'phase' => 'SINGLE_PHASE',//phase
                            'type' => 'rack_pdu',//type
                            'power_iq_rack_id' => $pdu['rack_id'],//power_iq_rack_id
                            'rack_id' => $pdu['rack_id'],//rack_id
                            'volt' => (float)$voltage,
                            'current' => (float)$current,
                            'power' => (float)$power,
                            'connectivity' => (float)$connectivity,
                            'rtime' =>  new Expression('NOW()') //rtime
                        ));
                    }
                    $logger->log(Logger::INFO, 'current: ' . $current);
                    $logger->log(Logger::INFO, 'voltage: ' . $voltage);
                    $logger->log(Logger::INFO, 'power: ' . $power);

                    $con->commit();
                } catch(\Exception $e) {
                    $con->rollback();
                    /*$p = $e->getPrevious();
                    if($p) {
                        $e = $p;
                    }*/
                    $logger->log(Logger::ERR, "unable to finish importing power-iq pdus on batch {$batch} : " . $e->getMessage());
                    $logger->log(Logger::ERR, $e->getTraceAsString());
                }
            }
        }
    }

    protected function processEatonPdus()
    {
        $sm = $this->getServiceLocator();
        $config = $sm->get('Config');
        $db = $sm->get('db');
        $logger = $sm->get('console_logger');

        $power_pdus_mdl = PowerPdus::factory($sm);
        $asset_devices_mdl = AssetDevices::factory($sm);
        $asset_device_ips_mdl = AssetDeviceIps::factory($sm);

        $batch = 0;
        $batchsize = 50;
        $last_id = null;
        $more = false;

        $con = $db->getDriver()->getConnection();


        $pdus = $asset_devices_mdl->filter(
            array(
                'type' => 'pdu',
                'manufacturer' => 'Eaton',
                'model' => 'EMA107-10'

            ),
            array(),
            array(),
            $pdu_total
        )->toArray();

        $logger->log(Logger::INFO, "processing power-iq pdus " . sizeof($pdus));
        foreach ($pdus as $pdu) {
            $connectivity = 0;

            $ips = $asset_device_ips_mdl->filter(
                array(
                    'device_id' => $pdu['id']
                ),
                array(),
                array(),
                $ip_total
            )->toArray();

            // Read values from Power Bars
            if (!empty($ips)) {
                // IP, password, oid (key)
                $current = snmpget($ips[0]['ip'], "public", ".1.3.6.1.4.1.534.6.6.7.3.3.1.4.0.1.1");
                $logger->log(Logger::INFO, $pdu['lab'] . ' -> ' .  $pdu['rack'] . ' -> ' . $ips[0]['ip'] . ' underway!');
                if ($current) {
                    $current = (float) ltrim($current, "INTEGER: ");
                    $current /= 1000;
                    $logger->log(Logger::INFO, 'current - ' . gettype($current) . ': ' . $current);
                }

                $voltage = snmpget($ips[0]['ip'], "public", ".1.3.6.1.4.1.534.6.6.7.3.2.1.3.0.1.1");
                if ($voltage) {
                    $voltage = (float) ltrim($voltage, 'INTEGER:');
                    $logger->log(Logger::INFO, 'voltage - ' . gettype($voltage) . ': ' . $voltage);
                }

                $power = snmpget($ips[0]['ip'], "public", ".1.3.6.1.4.1.534.6.6.7.3.4.1.4.0.1.1");
                if ($power) {
                    $power = (float) ltrim($power, 'INTEGER:');
                    $logger->log(Logger::INFO, 'power - ' . gettype($power) . ': ' . $power);
                }

                if ($current || $voltage || $power > 0) {
                    $connectivity = 1;
                }
                // $logger->log(Logger::INFO, 'power - ' . gettype($power) . ': ' . $power);

                $power_pdu = $power_pdus_mdl->get(array(
                    'power_iq_pdu_id' => $pdu['id']
                ));

                try {
                    $con->beginTransaction();

                    if ($power_pdu === false) {
                        $power_pdus_mdl->insert(
                            $pdu['id'], //power_iq_pdu_id
                            $ips[0]['ip'], //ip
                            $pdu['name'], // hostname
                            'Eaton', // manufacturer
                            'EMA107-10', //model
                            'SINGLE_PHASE', //phase
                            'rack_pdu', //type
                            $pdu['rack_id'], //power_iq_rack_id
                            $pdu['rack_id'], //rack_id
                            (float) $voltage, //voltage
                            (float) $current, //current
                            (float) $power, //power
                            $connectivity, //connectivity
                            new Expression('NOW()') //rtime
                        );
                    } else {
                        $power_pdus_mdl->update($power_pdu->id, array(
                            'power_iq_pdu_id' => $pdu['id'],
                            'ip' => $ips[0]['ip'], //ip
                            'hostname' => $pdu['name'],
                            'manufacturer' => 'Eaton', // manufacturer
                            'model' => 'EMA107-10', //model
                            'phase' => 'SINGLE_PHASE', //phase
                            'type' => 'rack_pdu', //type
                            'power_iq_rack_id' => $pdu['rack_id'], //power_iq_rack_id
                            'rack_id' => $pdu['rack_id'], //rack_id
                            'volt' => (float) $voltage,
                            'current' => (float) $current,
                            'power' => (float) $power,
                            'connectivity' => (float) $connectivity,
                            'rtime' =>  new Expression('NOW()') //rtime
                        ));
                    }
                    $logger->log(Logger::INFO, 'current: ' . $current);
                    $logger->log(Logger::INFO, 'power: ' . $power);

                    $con->commit();
                } catch (\Exception $e) {
                    $con->rollback();
                    //  $p = $e->getPrevious();
                    //if($p) {
                    //  $e = $p;
                    //}
                    $logger->log(Logger::ERR, "unable to finish importing power-iq pdus on batch {$batch} : " . $e->getMessage());
                    $logger->log(Logger::ERR, $e->getTraceAsString());
                }
            }
        }
    }
}
