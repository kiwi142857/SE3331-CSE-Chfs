FROM ubuntu:22.04
#FROM ubuntu:18.04
# Set up proxy

CMD bash

# Install Ubuntu packages.
# Please add packages in alphabetical order.
ARG DEBIAN_FRONTEND=noninteractive
RUN apt-get -y update
RUN apt-get -y install \
  build-essential \
  fuse libfuse-dev 
RUN apt-get install -y sudo \
  clang-14 \
  clang-format-14 \
  clang-tidy-14 \
  cmake \
  doxygen \
  git \
  g++-12 \
  pkg-config \
  zlib1g-dev 
RUN apt-get -y install curl zsh 

RUN update-alternatives --install /usr/bin/cc cc /usr/bin/clang-14 100 && \
  update-alternatives --install /usr/bin/c++ c++ /usr/bin/clang++-14 100 && \
  ln -s /usr/bin/clang-format-14 /usr/bin/clang-format

COPY mypasswd /tmp

RUN useradd --no-log-init -r -m -g sudo stu

RUN cat /tmp/mypasswd | chpasswd

USER stu

ENV http_proxy http://127.0.0.1:7890
ENV https_proxy http://127.0.0.1:7890

RUN sh -c "$(curl -fsSL https://install.ohmyz.sh/)"
RUN git clone https://github.com/zsh-users/zsh-autosuggestions ${ZSH_CUSTOM:-~/.oh-my-zsh/custom}/plugins/zsh-autosuggestions
RUN git clone https://github.com/zsh-users/zsh-syntax-highlighting.git ${ZSH_CUSTOM:-~/.oh-my-zsh/custom}/plugins/zsh-syntax-highlighting

# plugins=(git zsh-autosuggestions zsh-syntax-highlighting z extract web-search)

# 添加插件到 .zshrc 并将 /usr/bin 添加到系统路径
RUN echo 'export PATH=$PATH:/usr/bin' >> ~/.zshrc && \
  echo 'plugins=(git zsh-autosuggestions zsh-syntax-highlighting)' >> ~/.zshrc && \
  echo 'source $ZSH/oh-my-zsh.sh' >> ~/.zshrc

WORKDIR /home/stu/