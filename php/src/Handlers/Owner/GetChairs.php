<?php

declare(strict_types=1);

namespace IsuRide\Handlers\Owner;

use Fig\Http\Message\StatusCodeInterface;
use IsuRide\Database\Model\ChairWithDetail;
use IsuRide\Database\Model\Owner;
use IsuRide\Handlers\AbstractHttpHandler;
use IsuRide\Model\OwnerGetChairs200Response;
use IsuRide\Model\OwnerGetChairs200ResponseChairsInner;
use IsuRide\Response\ErrorResponse;
use PDO;
use PDOException;
use Psr\Http\Message\ResponseInterface;
use Psr\Http\Message\ServerRequestInterface;

class GetChairs extends AbstractHttpHandler
{
    public function __construct(
        private readonly PDO $db,
    ) {
    }

    /**
     * @param ServerRequestInterface $request
     * @param ResponseInterface $response
     * @param array<string, string> $args
     * @return ResponseInterface
     * @throws \Exception
     */
    public function __invoke(
        ServerRequestInterface $request,
        ResponseInterface $response,
        array $args
    ): ResponseInterface {
        $owner = $request->getAttribute('owner');
        assert($owner instanceof Owner);
        /** @var ChairWithDetail[] $chairs */
        $chairs = [];
        try {
            // owner_idに対応するchair idのみ取る
            // owner_id に対応するすべての chair_id を取得
            $stmt = $this->db->prepare('SELECT id FROM chairs WHERE owner_id = ?');
            $stmt->execute([$owner->id]);
            $chairIds = $stmt->fetchAll(PDO::FETCH_COLUMN);

            // `chairIds`を`WHERE IN`で利用   SQLプレースホルダの準備
            $inQuery = implode(',', array_fill(0, count($chairIds), '?'));

            $stmt = $this->db->prepare(
                <<<SQL
SELECT c.id,
       c.owner_id,
       c.name,
       c.access_token,
       c.model,
       c.is_active,
       c.created_at,
       c.updated_at,
       IFNULL(dt.total_distance, 0) AS total_distance,
       dt.total_distance_updated_at
FROM chairs c
LEFT JOIN (
    SELECT chair_id,
           SUM(IFNULL(distance, 0)) AS total_distance,
           MAX(created_at) AS total_distance_updated_at
    FROM (
        SELECT chair_id,
               created_at,
               ABS(latitude - LAG(latitude) OVER (PARTITION BY chair_id ORDER BY created_at)) +
               ABS(longitude - LAG(longitude) OVER (PARTITION BY chair_id ORDER BY created_at)) AS distance
        FROM chair_locations
        WHERE chair_id IN ($inQuery)
    ) tmp
    GROUP BY chair_id
) dt ON dt.chair_id = c.id
WHERE c.id IN ($inQuery)
SQL
            );

            // プレースホルダに chairIds を2度挿入
            $stmt->execute(array_merge($chairIds, $chairIds));
            $chairs = $stmt->fetchAll(PDO::FETCH_ASSOC);
        } catch (PDOException $e) {
            return (new ErrorResponse())->write(
                $response,
                StatusCodeInterface::STATUS_INTERNAL_SERVER_ERROR,
                $e
            );
        }
        $res = new OwnerGetChairs200Response();
        $ownerChairs = [];
        foreach ($chairs as $row) {
            $chair = new ChairWithDetail(
                id: $row['id'],
                ownerId: $row['owner_id'],
                name: $row['name'],
                accessToken: $row['access_token'],
                model: $row['model'],
                isActive: (bool)$row['is_active'],
                createdAt: $row['created_at'],
                updatedAt: $row['updated_at'],
                totalDistance: (int)$row['total_distance'],
                totalDistanceUpdatedAt: $row['total_distance_updated_at']
            );
            $ownerChair = new OwnerGetChairs200ResponseChairsInner();
            $ownerChair->setId($chair->id)
                ->setName($chair->name)
                ->setModel($chair->model)
                ->setActive($chair->isActive)
                ->setRegisteredAt($chair->createdAtUnixMilliseconds())
                ->setTotalDistance($chair->totalDistance);
            if ($chair->isTotalDistanceUpdatedAt()) {
                $ownerChair->setTotalDistanceUpdatedAt($chair->totalDistanceUpdatedAtUnixMilliseconds());
            }
            $ownerChairs[] = $ownerChair;
        }
        return $this->writeJson($response, $res->setChairs($ownerChairs));
    }
}
