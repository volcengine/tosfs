# ByteDance Volcengine EMR, Copyright 2024.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# require packages:
# pip install lightning
# pip install torch
# pip install torchvision

import os
import time

import fsspec
import lightning as L
import torch
import torchvision
from PIL import Image
from torchdata.datapipes.iter import FSSpecFileLister, FSSpecFileOpener
from tos import EnvCredentialsProvider

fsspec.register_implementation("tos", "tosfs.TosFileSystem")

class VisionModel(L.LightningModule):
    def __init__(
            self,
            dataset: torch.utils.data.Dataset,
            model_name: str,
            batch_size: int,
            num_workers: int,
    ):
        super().__init__()

        ctor = getattr(torchvision.models, model_name)
        self.model = ctor(weights=None)
        self.dataset = dataset
        self.batch_size = batch_size
        self.num_workers = num_workers
        self.epoch_start_time = None
        self.epoch_images = 0

        self.loss_fn = torch.nn.CrossEntropyLoss()

    def configure_optimizers(self):
        return torch.optim.AdamW(self.parameters(), lr=1e-3)

    def train_dataloader(self) -> torch.utils.data.DataLoader:
        if self.epoch_start_time is None:
            self.epoch_start_time = time.perf_counter()
        return torch.utils.data.DataLoader(
            self.dataset, batch_size=self.batch_size, num_workers=self.num_workers, shuffle=False,
        )

    def forward(self, imgs):
        return self.model(imgs)

    def training_step(self, batch, batch_idx):
        imgs, labels = batch
        self.epoch_images += len(imgs)
        preds = self.forward(imgs)
        loss = self.loss_fn(preds, labels)
        self.log('train_loss', loss)
        return loss

    def on_train_epoch_end(self):
        t = time.perf_counter() - self.epoch_start_time
        self.log('throughput', self.epoch_images / t)
        print(f'{self.epoch_images} images in {t:.2f}s = {self.epoch_images / t:.2f} images/sec')
        self.epoch_start_time = time.perf_counter()
        self.epoch_images = 0


def load_image(sample):
    to_tensor = torchvision.transforms.ToTensor()
    return (to_tensor(Image.open(sample['.jpg'])), int(sample['.cls'].read()))


if __name__ == '__main__':

    kwargs = {
        'endpoint_url': os.environ.get("TOS_ENDPOINT"),
        'credentials_provider' : EnvCredentialsProvider(),
        'region': 'cn-beijing'
    }

    dataset_uri = 'tos://your-bucket/your-dataset/'

    file_lister = FSSpecFileLister(root=dataset_uri, **kwargs)
    file_lister = file_lister.sharding_filter()
    iterable_dataset = FSSpecFileOpener(file_lister, mode='rb', **kwargs)
    iterable_dataset = iterable_dataset.load_from_tar().webdataset().map(load_image)

    L.seed_everything(21, True)
    trainer = L.Trainer(
        max_epochs=3, precision='16-mixed',
        enable_checkpointing=False,
    )

    model = VisionModel(
        iterable_dataset, model_name='resnet50', batch_size=64, num_workers=1)

    start = time.perf_counter()
    trainer.fit(model)
    end = time.perf_counter()
    print(end - start)
